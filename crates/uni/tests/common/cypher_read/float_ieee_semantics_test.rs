// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Float comparison, equality and equivalence follow openCypher, not the IEEE
//! total order Arrow's kernels use.
//!
//! CIP2016-06-14 (comparability and equality): floats compare numerically, so
//! `-0.0 = 0.0`; every `<`, `<=`, `>`, `>=` involving `NaN` is false, `NaN = x`
//! is false and `NaN <> x` is true. Equivalence (`DISTINCT`, grouping) is
//! equality except that two `NaN`s are equivalent. Orderability puts `NaN`
//! after positive infinity.
//!
//! Arrow's comparison kernels order `f64` by `total_cmp`, under which
//! `-0.0 < 0.0`, `NaN = NaN`, a negative-signed NaN sorts below `-inf` and a
//! positive one above `+inf`. Every native float comparison inherited that:
//! measured before the fix, `RETURN -0.0 = 0.0` was false, `0 > -0.0` true,
//! `MATCH (n) WHERE n.f < 0.0` returned the `0.0 / 0.0` row (a negative NaN on
//! x86), `n.f = n.f` held for it, `n.f = 0.0` missed the `-0.0` row, the
//! self-join `n.f = m.f` paired the NaN row with itself and missed `-0.0` with
//! `0.0`, and `-0.0` / `0.0` were two groups.
//!
//! The oracle is Rust's own `f64` operators, which are IEEE comparisons, so
//! every expectation is computed rather than transcribed. Each check runs with
//! the rows in L0 and again after a flush.

use uni_db::{DataType, IndexType, ScalarType, Uni, UniConfig, Value};

/// The stored float rows: `(id, f)`.
///
/// Both NaN signs: total order sends a negative NaN below everything and a
/// positive one above, so each sign catches a different wrong answer.
fn float_rows() -> Vec<(i64, Option<f64>)> {
    vec![
        (1, Some(-0.0)),
        (2, Some(0.0)),
        (3, Some(f64::NAN)),
        (4, Some(-f64::NAN)),
        (5, Some(1.0)),
        (6, Some(-1.0)),
        (7, Some(f64::INFINITY)),
        (8, Some(f64::NEG_INFINITY)),
        (9, None),
        (10, Some(2.5)),
    ]
}

/// Bounds a predicate compares against, as parameters.
fn bounds() -> Vec<Value> {
    vec![
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Int(0),
        Value::Float(f64::NAN),
        Value::Float(-f64::NAN),
        Value::Float(1.0),
        Value::Int(-1),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
    ]
}

const OPS: [&str; 6] = ["=", "<>", "<", "<=", ">", ">="];

/// IEEE comparison — the openCypher semantics for two numbers.
fn ieee(l: f64, op: &str, r: f64) -> bool {
    match op {
        "=" => l == r,
        "<>" => l != r,
        "<" => l < r,
        "<=" => l <= r,
        ">" => l > r,
        ">=" => l >= r,
        _ => unreachable!("{op}"),
    }
}

fn as_f64(v: &Value) -> f64 {
    match v {
        Value::Float(f) => *f,
        Value::Int(i) => *i as f64,
        other => panic!("not a number: {other:?}"),
    }
}

fn cfg() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

/// `:N {id, f, x, g}` — `f` FLOAT, `x` INT (`f` truncated where finite),
/// `g` an indexed copy of `f`.
async fn setup(flush: bool) -> Uni {
    let db = Uni::in_memory().config(cfg()).build().await.unwrap();
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property_nullable("f", DataType::Float64)
        .property_nullable("g", DataType::Float64)
        .property_nullable("x", DataType::Int64)
        .index("g", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await
        .unwrap();
    let session = db.session();
    let tx = session.tx().await.unwrap();
    for (id, f) in float_rows() {
        let fv = f.map_or(Value::Null, Value::Float);
        let xv = match f {
            Some(f) if f.is_finite() => Value::Int(f as i64),
            _ => Value::Null,
        };
        tx.execute_with("CREATE (:N {id: $id, f: $f, g: $f, x: $x})")
            .param("id", Value::Int(id))
            .param("f", fv)
            .param("x", xv)
            .run()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    if flush {
        db.flush().await.unwrap();
    }
    db
}

async fn ids(db: &Uni, cypher: &str, b: &Value) -> Vec<i64> {
    let rows = db
        .session()
        .query_with(cypher)
        .param("b", b.clone())
        .fetch_all()
        .await
        .unwrap_or_else(|e| panic!("{cypher} with $b = {b:?}: {e}"));
    let mut out: Vec<i64> = rows
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id").unwrap())
        .collect();
    out.sort_unstable();
    out
}

/// `prop op $b` over every operator and bound, through the scan filter, a
/// filter above a projection, the indexed column, and as a projected value.
#[tokio::test]
async fn property_comparisons_follow_ieee() {
    for flush in [false, true] {
        let db = setup(flush).await;
        let mut checked = 0;
        for op in OPS {
            for b in bounds() {
                let bf = as_f64(&b);
                let want: Vec<i64> = float_rows()
                    .into_iter()
                    .filter(|(_, f)| f.is_some_and(|f| ieee(f, op, bf)))
                    .map(|(id, _)| id)
                    .collect();
                let want_x: Vec<i64> = float_rows()
                    .into_iter()
                    .filter(|(_, f)| {
                        f.is_some_and(|f| f.is_finite() && ieee((f as i64) as f64, op, bf))
                    })
                    .map(|(id, _)| id)
                    .collect();
                for (q, want) in [
                    (
                        format!("MATCH (n:N) WHERE n.f {op} $b RETURN n.id AS id"),
                        &want,
                    ),
                    (
                        format!("MATCH (n:N) WHERE n.g {op} $b RETURN n.id AS id"),
                        &want,
                    ),
                    (
                        format!("MATCH (n:N) WITH n.id AS id, n.f AS f WHERE f {op} $b RETURN id"),
                        &want,
                    ),
                    (
                        format!("MATCH (n:N) WITH n, $b AS b WHERE b {op} n.f RETURN n.id AS id"),
                        // `b op f` is the mirror of `f op' b`.
                        &float_rows()
                            .into_iter()
                            .filter(|(_, f)| f.is_some_and(|f| ieee(bf, op, f)))
                            .map(|(id, _)| id)
                            .collect(),
                    ),
                    (
                        format!("MATCH (n:N) WHERE n.x {op} $b RETURN n.id AS id"),
                        &want_x,
                    ),
                ] {
                    assert_eq!(
                        &ids(&db, &q, &b).await,
                        want,
                        "flush={flush}: {q}, $b = {b:?}"
                    );
                    checked += 1;
                }

                // As a value: null exactly for the null row.
                let q = format!("MATCH (n:N) RETURN n.id AS id, n.f {op} $b AS r");
                let rows = db
                    .session()
                    .query_with(&q)
                    .param("b", b.clone())
                    .fetch_all()
                    .await
                    .unwrap();
                for row in rows.rows() {
                    let id = row.get::<i64>("id").unwrap();
                    let f = float_rows().into_iter().find(|(i, _)| *i == id).unwrap().1;
                    let want = f.map_or(Value::Null, |f| Value::Bool(ieee(f, op, bf)));
                    assert_eq!(
                        row.value("r").unwrap(),
                        &want,
                        "flush={flush}: {q}, id {id}, $b = {b:?}"
                    );
                }
            }
        }
        // Discrimination floor: the matrix ran in full.
        assert_eq!(checked, OPS.len() * bounds().len() * 5);
    }
}

/// Literal comparisons, folded or not.
#[tokio::test]
async fn literal_zero_and_nan_comparisons() {
    let db = Uni::in_memory().build().await.unwrap();
    let q = "RETURN -0.0 = 0.0 AS a, -0.0 <> 0.0 AS b, -0.0 < 0.0 AS c, -0.0 <= 0.0 AS d, \
             0 > -0.0 AS e, 0 = -0.0 AS f, -0.0 >= 0 AS g, \
             0.0 / 0.0 = 0.0 / 0.0 AS h, 0.0 / 0.0 <> 0.0 / 0.0 AS i, 0.0 / 0.0 < 1 AS j";
    let res = db.session().query(q).await.unwrap();
    let row = &res.rows()[0];
    let want = [
        ("a", true),
        ("b", false),
        ("c", false),
        ("d", true),
        ("e", false),
        ("f", true),
        ("g", true),
        ("h", false),
        ("i", true),
        ("j", false),
    ];
    for (col, w) in want {
        assert_eq!(row.value(col).unwrap(), &Value::Bool(w), "{col} in {q}");
    }
}

/// Column against column: the cross product filter and the hash join that
/// replaces it agree with IEEE equality and ordering.
#[tokio::test]
async fn column_pairs_follow_ieee() {
    for flush in [false, true] {
        let db = setup(flush).await;
        for op in OPS {
            let mut want: Vec<(i64, i64)> = Vec::new();
            for (a, fa) in float_rows() {
                for (b, fb) in float_rows() {
                    if let (Some(fa), Some(fb)) = (fa, fb)
                        && ieee(fa, op, fb)
                    {
                        want.push((a, b));
                    }
                }
            }
            for q in [
                format!("MATCH (n:N), (m:N) WHERE n.f {op} m.f RETURN n.id AS a, m.id AS b"),
                format!(
                    "MATCH (n:N) WITH n.id AS a, n.f AS f MATCH (m:N) WHERE m.f {op} f \
                     RETURN m.id AS a, a AS b"
                ),
            ] {
                let rows = db.session().query(&q).await.unwrap();
                let mut got: Vec<(i64, i64)> = rows
                    .rows()
                    .iter()
                    .map(|r| (r.get::<i64>("a").unwrap(), r.get::<i64>("b").unwrap()))
                    .collect();
                got.sort_unstable();
                let mut want = want.clone();
                want.sort_unstable();
                assert_eq!(got, want, "flush={flush}: {q}");
            }
        }
    }
}

/// `IN` uses equality: `-0.0` and `0.0` match each other, `NaN` matches
/// nothing.
#[tokio::test]
async fn in_lists_follow_ieee_equality() {
    for flush in [false, true] {
        let db = setup(flush).await;
        for list in [
            vec![Value::Float(0.0)],
            vec![Value::Float(-0.0)],
            vec![Value::Int(0)],
            vec![Value::Float(f64::NAN)],
            vec![Value::Float(f64::NAN), Value::Float(1.0)],
        ] {
            let want: Vec<i64> = float_rows()
                .into_iter()
                .filter(|(_, f)| f.is_some_and(|f| list.iter().any(|v| f == as_f64(v))))
                .map(|(id, _)| id)
                .collect();
            for col in ["f", "g"] {
                let q = format!("MATCH (n:N) WHERE n.{col} IN $b RETURN n.id AS id");
                assert_eq!(
                    ids(&db, &q, &Value::List(list.clone())).await,
                    want,
                    "flush={flush}: {q}, $b = {list:?}"
                );
            }
            // No list here holds a null, so NOT IN is the plain complement
            // over non-null rows.
            let want_not: Vec<i64> = float_rows()
                .into_iter()
                .filter(|(id, f)| f.is_some() && !want.contains(id))
                .map(|(id, _)| id)
                .collect();
            let q = "MATCH (n:N) WHERE NOT n.f IN $b RETURN n.id AS id";
            assert_eq!(
                ids(&db, q, &Value::List(list.clone())).await,
                want_not,
                "flush={flush}: {q}, $b = {list:?}"
            );
        }
        // The literal forms of the zero lists.
        for (q, want) in [
            (
                "MATCH (n:N) WHERE n.f IN [0.0] RETURN n.id AS id",
                vec![1, 2],
            ),
            (
                "MATCH (n:N) WHERE n.f IN [-0.0] RETURN n.id AS id",
                vec![1, 2],
            ),
            ("MATCH (n:N) WHERE n.g = 0 RETURN n.id AS id", vec![1, 2]),
            ("MATCH (n:N) WHERE n.g = 0.0 RETURN n.id AS id", vec![1, 2]),
            ("MATCH (n:N) WHERE n.g = -0.0 RETURN n.id AS id", vec![1, 2]),
            (
                "MATCH (n:N) WHERE n.g IN [0, 2.5] RETURN n.id AS id",
                vec![1, 2, 10],
            ),
        ] {
            assert_eq!(ids(&db, q, &Value::Null).await, want, "flush={flush}: {q}");
        }
    }
}

/// Equivalence: `-0.0` and `0.0` are one value, every NaN is one value.
#[tokio::test]
async fn distinct_and_grouping_follow_equivalence() {
    // {±0}, {±NaN}, 1, -1, inf, -inf, 2.5 — null is not counted.
    const CLASSES: i64 = 7;
    for flush in [false, true] {
        let db = setup(flush).await;
        let s = db.session();

        let r = s
            .query("MATCH (n:N) RETURN count(DISTINCT n.f) AS c")
            .await
            .unwrap();
        assert_eq!(
            r.rows()[0].get::<i64>("c").unwrap(),
            CLASSES,
            "flush={flush}"
        );

        let r = s
            .query("MATCH (n:N) WHERE n.f IS NOT NULL RETURN DISTINCT n.f AS f")
            .await
            .unwrap();
        assert_eq!(r.rows().len() as i64, CLASSES, "flush={flush}: DISTINCT");

        let r = s
            .query("MATCH (n:N) WHERE n.f IS NOT NULL WITH DISTINCT n.f AS f RETURN count(*) AS c")
            .await
            .unwrap();
        assert_eq!(
            r.rows()[0].get::<i64>("c").unwrap(),
            CLASSES,
            "flush={flush}: WITH DISTINCT"
        );

        let r = s
            .query("MATCH (n:N) WHERE n.f IS NOT NULL RETURN n.f AS f, count(*) AS c")
            .await
            .unwrap();
        assert_eq!(r.rows().len() as i64, CLASSES, "flush={flush}: grouping");
        let mut counts: Vec<i64> = r
            .rows()
            .iter()
            .map(|r| r.get::<i64>("c").unwrap())
            .collect();
        counts.sort_unstable();
        assert_eq!(
            counts,
            vec![1, 1, 1, 1, 1, 2, 2],
            "flush={flush}: group sizes"
        );

        let r = s
            .query(
                "MATCH (n:N) WHERE n.f IS NOT NULL \
                 RETURN size(collect(DISTINCT n.f)) AS c",
            )
            .await
            .unwrap();
        assert_eq!(
            r.rows()[0].get::<i64>("c").unwrap(),
            CLASSES,
            "flush={flush}: collect"
        );
    }

    let db = Uni::in_memory().build().await.unwrap();
    let s = db.session();
    for (q, want) in [
        ("RETURN -0.0 AS x UNION RETURN 0.0 AS x", 1),
        (
            "UNWIND [-0.0, 0.0, 0.0 / 0.0, 0.0 / 0.0, 1.0] AS x RETURN DISTINCT x",
            3,
        ),
        (
            "UNWIND [-0.0, 0.0, 0.0 / 0.0, 0.0 / 0.0, 1.0] AS x RETURN x, count(*) AS c",
            3,
        ),
    ] {
        let r = s.query(q).await.unwrap();
        assert_eq!(r.rows().len(), want, "{q}: {:?}", r.rows());
    }
}

/// ORDER BY puts every NaN after positive infinity, whatever its sign bit.
#[tokio::test]
async fn order_by_puts_nan_after_infinity() {
    for flush in [false, true] {
        let db = setup(flush).await;
        for (q, desc) in [
            ("MATCH (n:N) RETURN n.id AS id, n.f AS f ORDER BY f", false),
            (
                "MATCH (n:N) RETURN n.id AS id, n.f AS f ORDER BY f DESC",
                true,
            ),
        ] {
            let r = db.session().query(q).await.unwrap();
            // Rank: -inf < -1 < ±0 < 1 < 2.5 < inf < NaN < null.
            let rank = |f: Option<f64>| -> u8 {
                match f {
                    None => 7,
                    Some(f) if f.is_nan() => 6,
                    Some(f) if f == f64::INFINITY => 5,
                    Some(f) if f == 2.5 => 4,
                    Some(f) if f == 1.0 => 3,
                    Some(f) if f == 0.0 => 2,
                    Some(f) if f == -1.0 => 1,
                    Some(_) => 0,
                }
            };
            let got: Vec<u8> = r
                .rows()
                .iter()
                .map(|row| {
                    let id = row.get::<i64>("id").unwrap();
                    rank(float_rows().into_iter().find(|(i, _)| *i == id).unwrap().1)
                })
                .collect();
            let mut want = got.clone();
            want.sort_unstable();
            if desc {
                want.reverse();
            }
            assert_eq!(got, want, "flush={flush}: {q}");
        }
    }
}

/// Simple `CASE` compares with equality.
#[tokio::test]
async fn simple_case_uses_ieee_equality() {
    let db = setup(false).await;
    let r = db
        .session()
        .query(
            "MATCH (n:N) WHERE CASE n.f WHEN 0.0 THEN true ELSE false END \
             RETURN n.id AS id ORDER BY id",
        )
        .await
        .unwrap();
    let got: Vec<i64> = r
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id").unwrap())
        .collect();
    assert_eq!(got, vec![1, 2]);
}
