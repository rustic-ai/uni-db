//! Regression tests for <https://github.com/rustic-ai/uni-db/issues/94>
//!
//! A property-access expression in a `YIELD KEY` position (e.g. `YIELD KEY a.id`)
//! must produce a correctly-typed, correctly-named scalar KEY column. Before the
//! fix it (A) crashed at runtime when the KEY alias collided with a MATCH node
//! variable (`LargeBinary → UInt64` cast), (B) silently collided two bare `id`
//! columns, and lost integer typing to `Float`.

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use uni_db::{DataType, Uni, Value};

type Row = HashMap<String, Value>;

async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .property("id", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Node {id: 0})-[:EDGE]->(:Node {id: 1})")
        .await?;
    tx.execute("CREATE (:Node {id: 1})-[:EDGE]->(:Node {id: 2})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// Collect the `(left, right)` integer pairs from a rule's derived facts,
/// reading the two KEY columns by name.
fn int_pairs(rows: &[Row], left: &str, right: &str) -> HashSet<(i64, i64)> {
    rows.iter()
        .filter_map(|row| match (row.get(left), row.get(right)) {
            (Some(Value::Int(l)), Some(Value::Int(r))) => Some((*l, *r)),
            _ => None,
        })
        .collect()
}

fn expected_pairs() -> HashSet<(i64, i64)> {
    HashSet::from([(0, 1), (1, 2)])
}

#[tokio::test]
async fn variant_a_property_key_with_colliding_alias() -> Result<()> {
    // The KEY aliases `a`/`b` collide with the MATCH node variables `a`/`b`.
    // This previously crashed with `Casting from LargeBinary to UInt64`.
    let db = setup().await?;
    let session = db.session();
    let program = r#"
        CREATE RULE reachable AS
            MATCH (a:Node)-[:EDGE]->(b:Node)
            YIELD KEY a.id AS a, KEY b.id AS b

        QUERY reachable RETURN *
    "#;
    let result = session.locy(program).await?;
    let empty = vec![];
    let facts = result
        .derived_rule("reachable")
        .await
        .unwrap()
        .unwrap_or(&empty);
    assert_eq!(
        int_pairs(facts, "a", "b"),
        expected_pairs(),
        "property KEYs must yield integer pairs; facts={facts:?}"
    );
    Ok(())
}

#[tokio::test]
async fn variant_a2_property_key_noncolliding_alias_preserves_int() -> Result<()> {
    // Non-colliding aliases never crashed, but integer ids surfaced as Float.
    let db = setup().await?;
    let session = db.session();
    let program = r#"
        CREATE RULE reachable AS
            MATCH (a:Node)-[:EDGE]->(b:Node)
            YIELD KEY a.id AS x, KEY b.id AS y

        QUERY reachable RETURN *
    "#;
    let result = session.locy(program).await?;
    let empty = vec![];
    let facts = result
        .derived_rule("reachable")
        .await
        .unwrap()
        .unwrap_or(&empty);
    assert!(!facts.is_empty(), "expected derived facts");
    for row in facts {
        assert!(
            matches!(row.get("x"), Some(Value::Int(_))),
            "x must stay Int, not Float; got {:?}",
            row.get("x")
        );
    }
    assert_eq!(int_pairs(facts, "x", "y"), expected_pairs());
    Ok(())
}

#[tokio::test]
async fn variant_b_bare_property_keys_decollide() -> Result<()> {
    // Bare `KEY a.id, KEY b.id` previously collided on a single `id` column;
    // they must now de-collide to qualified `a_id` / `b_id`.
    let db = setup().await?;
    let session = db.session();
    let program = r#"
        CREATE RULE reachable AS
            MATCH (a:Node)-[:EDGE]->(b:Node)
            YIELD KEY a.id, KEY b.id

        QUERY reachable RETURN *
    "#;
    let result = session.locy(program).await?;
    let empty = vec![];
    let facts = result
        .derived_rule("reachable")
        .await
        .unwrap()
        .unwrap_or(&empty);
    assert!(!facts.is_empty(), "expected derived facts");
    for row in facts {
        assert!(
            row.contains_key("a_id") && row.contains_key("b_id"),
            "bare property KEYs must de-collide to a_id/b_id; keys={:?}",
            row.keys().collect::<Vec<_>>()
        );
    }
    assert_eq!(int_pairs(facts, "a_id", "b_id"), expected_pairs());
    Ok(())
}

#[tokio::test]
async fn control_whole_node_key_still_works() -> Result<()> {
    // Regression guard for the whole-node KEY path (must keep its UInt64 VID
    // typing after the property-KEY type-inference change).
    let db = setup().await?;
    let session = db.session();
    let program = r#"
        CREATE RULE reachable AS
            MATCH (a:Node)-[:EDGE]->(b:Node)
            YIELD KEY a, KEY b

        QUERY reachable RETURN *
    "#;
    let result = session.locy(program).await?;
    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);
    assert_eq!(
        rows.len(),
        2,
        "expected two whole-node pairs; rows={rows:?}"
    );
    for row in rows {
        assert!(matches!(row.get("a"), Some(Value::Node(_))));
        assert!(matches!(row.get("b"), Some(Value::Node(_))));
    }
    Ok(())
}
