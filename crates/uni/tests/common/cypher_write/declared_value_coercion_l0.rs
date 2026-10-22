// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A value written into a declared column reads back the same before and
//! after a flush.
//!
//! `DataType::accepts` admits a few lossless widenings (`Int` into a float
//! column, a numeric `List` into a `VECTOR`). The write path used to store
//! the accepted value as given, so L0 held `List[Int]` and `Int` while the
//! flushed row held `Vector` and `Float`: `RETURN n` and `properties(n)`
//! changed type across a flush, and a filter or a comparison could answer
//! differently depending on when it ran. The write path now stores the
//! declared column's own form. Indexing a `Vector` behaves like indexing a
//! list.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Session, Uni, Value};

async fn setup() -> Result<(Uni, Session)> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("V")
        .property("id", DataType::Int64)
        .property_nullable("e", DataType::Vector { dimensions: 2 })
        .property_nullable("f", DataType::Float64)
        .property_nullable("g", DataType::Float32)
        .property_nullable(
            "m",
            DataType::List(Box::new(DataType::Vector { dimensions: 2 })),
        )
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute(
        "CREATE (:V {id: 1, e: [1, 0], f: 1, g: 2, m: [[1, 2], [3, 4]]}), \
                (:V {id: 2, e: [0.5, 0.25], f: 2.5}), \
                (:V {id: 3})",
    )
    .await?;
    tx.commit().await?;
    Ok((db, s))
}

async fn rows(s: &Session, q: &str) -> Result<Vec<Vec<Value>>> {
    let res = s.query(q).await?;
    Ok(res.rows().iter().map(|r| r.values().to_vec()).collect())
}

/// Everything the readers can see of the rows, in one comparable shape.
async fn snapshot(s: &Session) -> Result<Vec<Vec<Value>>> {
    let mut out = rows(
        s,
        "MATCH (n:V) RETURN n.id, n.e, n.f, n.g, n.m, properties(n) ORDER BY n.id",
    )
    .await?;
    for r in rows(s, "MATCH (n:V) RETURN n ORDER BY n.id").await? {
        let Value::Node(node) = &r[0] else {
            panic!("RETURN n gave {:?}", r[0]);
        };
        let mut props: Vec<(String, Value)> = node
            .properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        props.sort_by(|a, b| a.0.cmp(&b.0));
        out.push(
            props
                .into_iter()
                .map(|(k, v)| Value::List(vec![Value::String(k), v]))
                .collect(),
        );
    }
    Ok(out)
}

#[tokio::test]
async fn declared_values_read_the_same_before_and_after_flush() -> Result<()> {
    let (_db, s) = setup().await?;
    let before = snapshot(&s).await?;
    // The declared form, not the literal's: a `Vector` and `Float`s.
    assert_eq!(
        before[0][1..5],
        [
            Value::Vector(vec![1.0, 0.0]),
            Value::Float(1.0),
            Value::Float(2.0),
            Value::List(vec![
                Value::Vector(vec![1.0, 2.0]),
                Value::Vector(vec![3.0, 4.0]),
            ]),
        ],
        "L0 row 1"
    );
    s.flush().await?;
    let after = snapshot(&s).await?;
    assert_eq!(before, after, "the rows changed across a flush");
    Ok(())
}

#[tokio::test]
async fn set_stores_the_declared_form() -> Result<()> {
    let (_db, s) = setup().await?;
    let tx = s.tx().await?;
    tx.execute("MATCH (n:V {id: 3}) SET n.e = [3, 4], n.f = 7")
        .await?;
    tx.execute("MATCH (n:V {id: 2}) SET n += {f: 9}").await?;
    tx.commit().await?;
    let before = rows(&s, "MATCH (n:V) RETURN n.e, n.f ORDER BY n.id").await?;
    assert_eq!(
        before[2],
        vec![Value::Vector(vec![3.0, 4.0]), Value::Float(7.0)]
    );
    assert_eq!(before[1][1], Value::Float(9.0));
    // The stored row, not just the projection: `properties(n)` and `RETURN n`.
    let snap_before = snapshot(&s).await?;
    let Value::Map(p3) = &snap_before[2][5] else {
        panic!("properties(n) gave {:?}", snap_before[2][5]);
    };
    assert_eq!(p3.get("e"), Some(&Value::Vector(vec![3.0, 4.0])));
    assert_eq!(p3.get("f"), Some(&Value::Float(7.0)));
    s.flush().await?;
    let after = rows(&s, "MATCH (n:V) RETURN n.e, n.f ORDER BY n.id").await?;
    assert_eq!(before, after);
    assert_eq!(
        snap_before,
        snapshot(&s).await?,
        "the rows changed across a flush"
    );
    Ok(())
}

#[tokio::test]
async fn a_vector_indexes_like_a_list() -> Result<()> {
    let (_db, s) = setup().await?;
    for stage in ["l0", "flushed"] {
        if stage == "flushed" {
            s.flush().await?;
        }
        let got = rows(
            &s,
            "MATCH (n:V) WHERE n.id < 3 \
             RETURN n.e[0], n.e[-1], n.e[2], n.e[null] ORDER BY n.id",
        )
        .await?;
        assert_eq!(
            got,
            vec![
                vec![
                    Value::Float(1.0),
                    Value::Float(0.0),
                    Value::Null,
                    Value::Null
                ],
                vec![
                    Value::Float(0.5),
                    Value::Float(0.25),
                    Value::Null,
                    Value::Null
                ],
            ],
            "{stage}"
        );
        // A non-integer index is the list error, not "cannot index into".
        let err = s
            .query_with("MATCH (n:V {id: 1}) RETURN n.e[$i]")
            .param("i", "a")
            .fetch_all()
            .await
            .expect_err("a string index into a vector must fail");
        assert!(
            err.to_string().contains("list index must be an integer"),
            "{stage}: {err}"
        );
    }
    Ok(())
}
