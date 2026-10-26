// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `UNWIND` evaluates its list expression the way `WITH` and `RETURN` do.
//!
//! The UNWIND operator used to evaluate the expression itself, row at a time,
//! with an evaluator that knew `range`, `keys`, `size`, `split` and the
//! temporal constructors. Every other function call evaluated to `[]` and
//! every other expression kind (CASE, list comprehension, slice, ...) to
//! `NULL` — both silently, both "no rows". So `MATCH (n) UNWIND labels(n) AS l
//! RETURN l` returned nothing and `UNWIND [id(n)] AS x` produced `[]`, while
//! `WITH labels(n) AS ls UNWIND ls AS l` was right.
//!
//! Separately, the planner typed the element of `UNWIND nodes(p)` /
//! `relationships(p)` as a scalar, so `labels(m)` / `type(m)` on it was
//! rejected at plan time.

use uni_db::{DataType, Uni, Value};

async fn exec(db: &Uni, cypher: &str) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    Ok(())
}

/// The first column of every row, in order.
async fn col(db: &Uni, cypher: &str) -> anyhow::Result<Vec<Value>> {
    let res = db.session().query(cypher).await?;
    Ok(res.rows().iter().map(|r| r.values()[0].clone()).collect())
}

fn strs(xs: &[&str]) -> Vec<Value> {
    xs.iter().map(|s| Value::String((*s).to_string())).collect()
}

fn ints(xs: &[i64]) -> Vec<Value> {
    xs.iter().map(|i| Value::Int(*i)).collect()
}

/// A declared label `D` and an undeclared multi-label node `S:T`, joined by a
/// relationship, optionally flushed to L1.
async fn graph(flushed: bool) -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("D")
        .property("x", DataType::Int64)
        .done()
        .apply()
        .await?;
    exec(&db, "CREATE (:D {x: 1})-[:R {w: 5}]->(:S:T {y: 2})").await?;
    if flushed {
        db.flush().await?;
    }
    Ok(db)
}

#[tokio::test]
async fn unwind_over_entity_functions() -> anyhow::Result<()> {
    for flushed in [false, true] {
        let db = graph(flushed).await?;
        let ctx = format!("flushed={flushed}");

        assert_eq!(
            col(&db, "MATCH (n) UNWIND labels(n) AS l RETURN l ORDER BY l").await?,
            strs(&["D", "S", "T"]),
            "{ctx}"
        );
        assert_eq!(
            col(&db, "MATCH (n:T) UNWIND [labels(n)] AS x RETURN x").await?,
            vec![Value::List(strs(&["S", "T"]))],
            "{ctx}"
        );
        assert_eq!(
            col(&db, "MATCH (n) UNWIND [id(n)] AS x RETURN count(x)").await?,
            ints(&[2]),
            "{ctx}"
        );
        assert_eq!(
            col(&db, "MATCH (n:D) UNWIND [properties(n)] AS p RETURN p.x").await?,
            ints(&[1]),
            "{ctx}"
        );
        assert_eq!(
            col(&db, "MATCH ()-[r]->() UNWIND [type(r)] AS t RETURN t").await?,
            strs(&["R"]),
            "{ctx}"
        );
        assert_eq!(
            col(
                &db,
                "MATCH (n) UNWIND range(0, size(labels(n)) - 1) AS i RETURN i ORDER BY i"
            )
            .await?,
            ints(&[0, 0, 1]),
            "{ctx}"
        );
        assert_eq!(
            col(
                &db,
                "MATCH (n) CALL { WITH n UNWIND labels(n) AS l RETURN l } RETURN l ORDER BY l"
            )
            .await?,
            strs(&["D", "S", "T"]),
            "{ctx}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn unwind_over_list_functions_and_expressions() -> anyhow::Result<()> {
    let db = graph(false).await?;
    let mut wrong = Vec::new();
    for (cypher, expected) in [
        ("UNWIND reverse([1, 2]) AS x RETURN x", ints(&[2, 1])),
        ("UNWIND tail([1, 2, 3]) AS x RETURN x", ints(&[2, 3])),
        ("UNWIND coalesce(null, [7]) AS x RETURN x", ints(&[7])),
        ("UNWIND [1, 2, 3][1..] AS x RETURN x", ints(&[2, 3])),
        (
            "UNWIND CASE WHEN true THEN [1, 2] END AS x RETURN x",
            ints(&[1, 2]),
        ),
        (
            "UNWIND [x IN [1, 2] | x * 10] AS y RETURN y",
            ints(&[10, 20]),
        ),
        (
            "MATCH (n:T) UNWIND [l IN labels(n) | toLower(l)] AS l RETURN l",
            strs(&["s", "t"]),
        ),
    ] {
        let got = col(&db, cypher).await?;
        if got != expected {
            wrong.push(format!("{cypher}: got {got:?}, expected {expected:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}

#[tokio::test]
async fn unwind_path_elements_are_entities() -> anyhow::Result<()> {
    for flushed in [false, true] {
        let db = graph(flushed).await?;
        let ctx = format!("flushed={flushed}");

        assert_eq!(
            col(
                &db,
                "MATCH p = ()-->() UNWIND nodes(p) AS m RETURN labels(m) AS l ORDER BY size(l)"
            )
            .await?,
            vec![Value::List(strs(&["D"])), Value::List(strs(&["S", "T"]))],
            "{ctx}"
        );
        assert_eq!(
            col(
                &db,
                "MATCH p = ()-->() UNWIND relationships(p) AS r RETURN type(r)"
            )
            .await?,
            strs(&["R"]),
            "{ctx}"
        );
        assert_eq!(
            col(&db, "MATCH p = ()-->() UNWIND nodes(p) AS m RETURN m.y").await?,
            vec![Value::Null, Value::Int(2)],
            "{ctx}"
        );
    }
    Ok(())
}

/// `keys()` and `properties()` leave out a declared property the entity does
/// not have.
///
/// A declared label's or edge type's columns reach the functions flattened
/// beside `_all_props`, which is null when nothing overflows. Only a non-null
/// `_all_props` marked the map as an entity, so the declared-but-unset columns
/// were read as the null entries of a plain map and listed: `keys(r)` was
/// `['status', 'year']` on a relationship with no properties. Routing UNWIND
/// through the expression compiler exposed it as `Graph8` [6] and [7] in
/// schema mode, but `RETURN keys(r)` had it all along.
#[tokio::test]
async fn keys_and_properties_skip_unset_declared_properties() -> anyhow::Result<()> {
    for flushed in [false, true] {
        let db = Uni::temporary().build().await?;
        db.schema()
            .label("A")
            .property_nullable("status", DataType::String)
            .done()
            .edge_type("KNOWS", &[], &[])
            .property_nullable("status", DataType::String)
            .property_nullable("year", DataType::String)
            .done()
            .apply()
            .await?;
        exec(&db, "CREATE (:A)-[:KNOWS]->(:A {status: 's'})").await?;
        if flushed {
            db.flush().await?;
        }
        let ctx = format!("flushed={flushed}");
        let empty_map = Value::Map(Default::default());

        assert_eq!(
            col(&db, "MATCH ()-[r:KNOWS]->() RETURN keys(r)").await?,
            vec![Value::List(vec![])],
            "{ctx}"
        );
        assert_eq!(
            col(&db, "MATCH ()-[r:KNOWS]->() RETURN properties(r)").await?,
            vec![empty_map.clone()],
            "{ctx}"
        );
        assert_eq!(
            col(&db, "MATCH ()-[r:KNOWS]->() UNWIND keys(r) AS k RETURN k").await?,
            vec![],
            "{ctx}"
        );
        assert_eq!(
            col(&db, "MATCH (n:A) RETURN keys(n) AS k ORDER BY size(k)").await?,
            vec![Value::List(vec![]), Value::List(strs(&["status"]))],
            "{ctx}"
        );
        assert_eq!(
            col(
                &db,
                "MATCH (n:A) WHERE n.status IS NULL RETURN properties(n)"
            )
            .await?,
            vec![empty_map],
            "{ctx}"
        );
        // An OPTIONAL MATCH that finds nothing: the entity is null, so are
        // its keys and properties.
        assert_eq!(
            col(
                &db,
                "OPTIONAL MATCH (:A {status: 'none'})-[r:KNOWS]->() RETURN keys(r)"
            )
            .await?,
            vec![Value::Null],
            "{ctx}"
        );
    }
    Ok(())
}
