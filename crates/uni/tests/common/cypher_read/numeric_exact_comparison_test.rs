// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integers and floats compare exactly, and `1` and `1.0` are one value.
//!
//! openCypher compares numbers by their mathematical value. Widening an
//! integer to `f64` is not exact above 2^53: `2^53 + 1` rounds to `2^53`, so
//! `9007199254740993 = 9007199254740992.0` came out true and orderings across
//! the boundary came out wrong. Equality between an integer and a float that
//! has the same value is true, so `1` and `1.0` are also one value for
//! `DISTINCT`, grouping and `UNION`; the first value seen is the one kept, as
//! Neo4j does.
//!
//! The oracle compares an `i64` with an `f64` exactly (through the float's
//! integral part and its fraction), so every expectation is computed. Each
//! check runs with the rows in L0 and again after a flush, through the plain
//! column, an indexed copy, a filter above a projection, a literal and a
//! parameter.

use std::cmp::Ordering;

use uni_db::{DataType, IndexType, ScalarType, Uni, UniConfig, Value};

const TWO_53: i64 = 1 << 53;
/// 2^63 as a float: one above `i64::MAX`.
const TWO_63: f64 = 9_223_372_036_854_775_808.0;

#[derive(Clone, Copy, Debug)]
enum Num {
    I(i64),
    F(f64),
}

impl Num {
    fn value(self) -> Value {
        match self {
            Num::I(i) => Value::Int(i),
            Num::F(f) => Value::Float(f),
        }
    }

    /// The number as Cypher source text.
    fn literal(self) -> String {
        match self {
            Num::I(i) => i.to_string(),
            Num::F(f) if f.is_nan() => "nan".to_string(),
            Num::F(f) if f == f64::INFINITY => "inf".to_string(),
            Num::F(f) if f == f64::NEG_INFINITY => "-inf".to_string(),
            // `{:?}` round-trips and always carries a `.` or an exponent.
            Num::F(f) => format!("{f:?}"),
        }
    }
}

/// `i` against `f`, exactly.
fn int_float_cmp(i: i64, f: f64) -> Option<Ordering> {
    if f.is_nan() {
        return None;
    }
    if f >= TWO_63 {
        return Some(Ordering::Less);
    }
    if f < -TWO_63 {
        return Some(Ordering::Greater);
    }
    let t = f.trunc();
    // `t` is integral and in `[-2^63, 2^63)`, so the cast is exact.
    match i.cmp(&(t as i64)) {
        Ordering::Equal => t.partial_cmp(&f),
        o => Some(o),
    }
}

fn exact_cmp(a: Num, b: Num) -> Option<Ordering> {
    match (a, b) {
        (Num::I(x), Num::I(y)) => Some(x.cmp(&y)),
        (Num::F(x), Num::F(y)) => x.partial_cmp(&y),
        (Num::I(x), Num::F(y)) => int_float_cmp(x, y),
        (Num::F(x), Num::I(y)) => int_float_cmp(y, x).map(Ordering::reverse),
    }
}

const OPS: [&str; 6] = ["=", "<>", "<", "<=", ">", ">="];

/// openCypher comparison of two numbers: incomparable (NaN) is false for
/// every operator but `<>`.
fn holds(a: Num, op: &str, b: Num) -> bool {
    let c = exact_cmp(a, b);
    match op {
        "=" => c == Some(Ordering::Equal),
        "<>" => c != Some(Ordering::Equal),
        "<" => c == Some(Ordering::Less),
        "<=" => matches!(c, Some(Ordering::Less | Ordering::Equal)),
        ">" => c == Some(Ordering::Greater),
        ">=" => matches!(c, Some(Ordering::Greater | Ordering::Equal)),
        _ => unreachable!("{op}"),
    }
}

fn ints() -> Vec<i64> {
    vec![
        TWO_53 - 1,
        TWO_53,
        TWO_53 + 1,
        TWO_53 + 2,
        -(TWO_53 + 1),
        i64::MAX,
        i64::MAX - 1,
        i64::MIN,
        i64::MIN + 1,
        0,
        1,
        -1,
    ]
}

fn floats() -> Vec<f64> {
    vec![
        TWO_53 as f64,
        (TWO_53 + 2) as f64,
        -(TWO_53 as f64),
        TWO_63,
        -TWO_63,
        0.0,
        -0.0,
        0.5,
        1.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ]
}

/// Every bound a predicate compares against.
fn bounds() -> Vec<Num> {
    ints()
        .into_iter()
        .map(Num::I)
        .chain(floats().into_iter().map(Num::F))
        .collect()
}

/// Rows: ids `1..` hold an integer in `i`/`gi`, ids `101..` a float in
/// `f`/`gf`.
fn int_rows() -> Vec<(i64, i64)> {
    ints().into_iter().zip(1..).map(|(v, id)| (id, v)).collect()
}

fn float_rows() -> Vec<(i64, f64)> {
    floats()
        .into_iter()
        .zip(101..)
        .map(|(v, id)| (id, v))
        .collect()
}

fn cfg() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

/// `:M {id, i, gi, f, gf}` (`gi`/`gf` indexed copies) and the schemaless
/// `:S {id, v}` holding every integer and float, so `v` is one column of
/// mixed type.
async fn setup(flush: bool) -> Uni {
    let db = Uni::in_memory().config(cfg()).build().await.unwrap();
    db.schema()
        .label("M")
        .property("id", DataType::Int64)
        .property_nullable("i", DataType::Int64)
        .property_nullable("gi", DataType::Int64)
        .property_nullable("f", DataType::Float64)
        .property_nullable("gf", DataType::Float64)
        .index("gi", IndexType::Scalar(ScalarType::BTree))
        .index("gf", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await
        .unwrap();
    let session = db.session();
    let tx = session.tx().await.unwrap();
    for (id, v) in int_rows() {
        tx.execute_with("CREATE (:M {id: $id, i: $v, gi: $v}), (:S {id: $id, v: $v})")
            .param("id", Value::Int(id))
            .param("v", Value::Int(v))
            .run()
            .await
            .unwrap();
    }
    for (id, v) in float_rows() {
        tx.execute_with("CREATE (:M {id: $id, f: $v, gf: $v}), (:S {id: $id, v: $v})")
            .param("id", Value::Int(id))
            .param("v", Value::Float(v))
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

/// Every row of `(id, value)` the comparison `value op b` keeps.
fn want_ids(rows: &[(i64, Num)], op: &str, b: Num) -> Vec<i64> {
    let mut out: Vec<i64> = rows
        .iter()
        .filter(|(_, v)| holds(*v, op, b))
        .map(|(id, _)| *id)
        .collect();
    out.sort_unstable();
    out
}

fn int_nums() -> Vec<(i64, Num)> {
    int_rows()
        .into_iter()
        .map(|(id, v)| (id, Num::I(v)))
        .collect()
}

fn float_nums() -> Vec<(i64, Num)> {
    float_rows()
        .into_iter()
        .map(|(id, v)| (id, Num::F(v)))
        .collect()
}

/// `prop op bound` for an integer and a float column against every bound,
/// as a parameter and as a literal, through the scan filter, the index, a
/// filter above a projection and the schemaless (CypherValue) column.
#[tokio::test]
async fn property_comparisons_are_exact() {
    for flush in [false, true] {
        let db = setup(flush).await;
        let all: Vec<(i64, Num)> = int_nums().into_iter().chain(float_nums()).collect();
        let mut checked = 0;
        let mut failures = Vec::new();
        for op in OPS {
            for b in bounds() {
                let lit = b.literal();
                let mirror = |rows: &[(i64, Num)]| -> Vec<i64> {
                    let mut out: Vec<i64> = rows
                        .iter()
                        .filter(|(_, v)| holds(b, op, *v))
                        .map(|(id, _)| *id)
                        .collect();
                    out.sort_unstable();
                    out
                };
                let cases = [
                    (
                        format!("MATCH (n:M) WHERE n.i {op} $b RETURN n.id AS id"),
                        want_ids(&int_nums(), op, b),
                    ),
                    (
                        format!("MATCH (n:M) WHERE n.gi {op} $b RETURN n.id AS id"),
                        want_ids(&int_nums(), op, b),
                    ),
                    (
                        format!("MATCH (n:M) WHERE n.f {op} $b RETURN n.id AS id"),
                        want_ids(&float_nums(), op, b),
                    ),
                    (
                        format!("MATCH (n:M) WHERE n.gf {op} $b RETURN n.id AS id"),
                        want_ids(&float_nums(), op, b),
                    ),
                    (
                        format!("MATCH (n:M) WHERE n.i {op} {lit} RETURN n.id AS id"),
                        want_ids(&int_nums(), op, b),
                    ),
                    (
                        format!("MATCH (n:M) WHERE n.gi {op} {lit} RETURN n.id AS id"),
                        want_ids(&int_nums(), op, b),
                    ),
                    (
                        format!("MATCH (n:M) WHERE n.f {op} {lit} RETURN n.id AS id"),
                        want_ids(&float_nums(), op, b),
                    ),
                    (
                        format!("MATCH (n:M) WHERE n.gf {op} {lit} RETURN n.id AS id"),
                        want_ids(&float_nums(), op, b),
                    ),
                    (
                        format!("MATCH (n:M) WHERE {lit} {op} n.gi RETURN n.id AS id"),
                        mirror(&int_nums()),
                    ),
                    (
                        format!("MATCH (n:M) WHERE $b {op} n.gf RETURN n.id AS id"),
                        mirror(&float_nums()),
                    ),
                    (
                        format!("MATCH (n:M) WITH n.id AS id, n.i AS v WHERE v {op} $b RETURN id"),
                        want_ids(&int_nums(), op, b),
                    ),
                    (
                        format!(
                            "MATCH (n:M) WITH n.id AS id, n.f AS v WHERE v {op} {lit} RETURN id"
                        ),
                        want_ids(&float_nums(), op, b),
                    ),
                    (
                        format!("MATCH (n:S) WHERE n.v {op} $b RETURN n.id AS id"),
                        want_ids(&all, op, b),
                    ),
                    (
                        format!("MATCH (n:S) WHERE n.v {op} {lit} RETURN n.id AS id"),
                        want_ids(&all, op, b),
                    ),
                ];
                for (q, want) in cases {
                    let got = ids(&db, &q, &b.value()).await;
                    if got != want {
                        failures.push(format!(
                            "flush={flush}: {q}, $b = {b:?}: got {got:?}, want {want:?}"
                        ));
                    }
                    checked += 1;
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} failures:\n{}",
            failures.len(),
            failures.join("\n")
        );
        // Discrimination floor: the matrix ran in full.
        assert_eq!(checked, OPS.len() * bounds().len() * 14);
    }
}

/// The comparison as a projected value, on both columns, and on literals.
#[tokio::test]
async fn comparison_values_are_exact() {
    let db = setup(false).await;
    let mut failures = Vec::new();
    for op in OPS {
        for b in bounds() {
            for (col, rows) in [("i", int_nums()), ("f", float_nums()), ("v", Vec::new())] {
                let q = if col == "v" {
                    format!("MATCH (n:S) RETURN n.id AS id, n.v {op} $b AS r")
                } else {
                    format!(
                        "MATCH (n:M) WHERE n.{col} IS NOT NULL RETURN n.id AS id, n.{col} {op} $b AS r"
                    )
                };
                let all: Vec<(i64, Num)> = int_nums().into_iter().chain(float_nums()).collect();
                let rows = if col == "v" { all } else { rows };
                let res = db
                    .session()
                    .query_with(&q)
                    .param("b", b.value())
                    .fetch_all()
                    .await
                    .unwrap_or_else(|e| panic!("{q}: {e}"));
                assert_eq!(res.rows().len(), rows.len(), "{q}");
                for row in res.rows() {
                    let id = row.get::<i64>("id").unwrap();
                    let v = rows.iter().find(|(i, _)| *i == id).unwrap().1;
                    let want = Value::Bool(holds(v, op, b));
                    if row.value("r").unwrap() != &want {
                        failures.push(format!(
                            "{q}, id {id} ({v:?}), $b = {b:?}: got {:?}",
                            row.value("r")
                        ));
                    }
                }
            }
        }
    }
    // Literal against literal.
    for op in OPS {
        for a in bounds() {
            for b in bounds() {
                let q = format!("RETURN {} {op} {} AS r", a.literal(), b.literal());
                let res = db
                    .session()
                    .query(&q)
                    .await
                    .unwrap_or_else(|e| panic!("{q}: {e}"));
                let want = Value::Bool(holds(a, op, b));
                if res.rows()[0].value("r").unwrap() != &want {
                    failures.push(format!("{q}: got {:?}", res.rows()[0].value("r")));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Integer column against float column: the cross-product filter and the
/// hash join on an equality.
#[tokio::test]
async fn column_pairs_are_exact() {
    for flush in [false, true] {
        let db = setup(flush).await;
        let mut failures = Vec::new();
        for op in OPS {
            let mut want: Vec<(i64, i64)> = Vec::new();
            for (a, x) in int_rows() {
                for (b, y) in float_rows() {
                    if holds(Num::I(x), op, Num::F(y)) {
                        want.push((a, b));
                    }
                }
            }
            want.sort_unstable();
            for q in [
                format!(
                    "MATCH (n:M), (m:M) WHERE n.i IS NOT NULL AND m.f IS NOT NULL AND n.i {op} m.f \
                     RETURN n.id AS a, m.id AS b"
                ),
                format!(
                    "MATCH (n:M) WHERE n.i IS NOT NULL WITH n.id AS a, n.i AS k \
                     MATCH (m:M) WHERE k {op} m.gf RETURN a, m.id AS b"
                ),
                format!(
                    "MATCH (n:S), (m:S) WHERE n.id < 100 AND m.id > 100 AND n.v {op} m.v \
                     RETURN n.id AS a, m.id AS b"
                ),
                // `IN` over a list of column values is a disjunction of `=`.
                if op == "=" {
                    "MATCH (n:M), (m:M) WHERE n.i IS NOT NULL AND m.f IS NOT NULL \
                     AND n.i IN [m.f, m.f] RETURN n.id AS a, m.id AS b"
                        .to_string()
                } else {
                    format!(
                        "MATCH (n:M), (m:M) WHERE n.i IS NOT NULL AND m.f IS NOT NULL \
                         AND n.i {op} m.f RETURN n.id AS a, m.id AS b"
                    )
                },
            ] {
                let rows = db
                    .session()
                    .query(&q)
                    .await
                    .unwrap_or_else(|e| panic!("{q}: {e}"));
                let mut got: Vec<(i64, i64)> = rows
                    .rows()
                    .iter()
                    .map(|r| (r.get::<i64>("a").unwrap(), r.get::<i64>("b").unwrap()))
                    .collect();
                got.sort_unstable();
                if got != want {
                    failures.push(format!("flush={flush}: {q}: got {got:?}, want {want:?}"));
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} failures:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}

/// `IN` is equality: a float list against the integer column and back.
#[tokio::test]
async fn in_lists_are_exact() {
    for flush in [false, true] {
        let db = setup(flush).await;
        for list in [
            vec![Num::F(TWO_53 as f64)],
            vec![Num::I(TWO_53 + 1)],
            vec![Num::F(TWO_63)],
            vec![Num::I(i64::MAX), Num::I(TWO_53 + 1)],
            vec![Num::F(1.0), Num::F(0.5)],
            vec![Num::F(TWO_53 as f64), Num::F(TWO_63)],
        ] {
            let lv = Value::List(list.iter().map(|n| n.value()).collect());
            let lit = format!(
                "[{}]",
                list.iter()
                    .map(|n| n.literal())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let member = |rows: Vec<(i64, Num)>| -> Vec<i64> {
                let mut out: Vec<i64> = rows
                    .into_iter()
                    .filter(|(_, v)| list.iter().any(|l| holds(*v, "=", *l)))
                    .map(|(id, _)| id)
                    .collect();
                out.sort_unstable();
                out
            };
            let all: Vec<(i64, Num)> = int_nums().into_iter().chain(float_nums()).collect();
            for (q, want) in [
                (
                    "MATCH (n:M) WHERE n.i IN $b RETURN n.id AS id".to_string(),
                    member(int_nums()),
                ),
                (
                    "MATCH (n:M) WHERE n.gi IN $b RETURN n.id AS id".to_string(),
                    member(int_nums()),
                ),
                (
                    "MATCH (n:M) WHERE n.f IN $b RETURN n.id AS id".to_string(),
                    member(float_nums()),
                ),
                (
                    "MATCH (n:M) WHERE n.gf IN $b RETURN n.id AS id".to_string(),
                    member(float_nums()),
                ),
                (
                    format!("MATCH (n:M) WHERE n.i IN {lit} RETURN n.id AS id"),
                    member(int_nums()),
                ),
                (
                    format!("MATCH (n:M) WHERE n.gi IN {lit} RETURN n.id AS id"),
                    member(int_nums()),
                ),
                (
                    format!("MATCH (n:M) WHERE n.gf IN {lit} RETURN n.id AS id"),
                    member(float_nums()),
                ),
                (
                    "MATCH (n:S) WHERE n.v IN $b RETURN n.id AS id".to_string(),
                    member(all.clone()),
                ),
                (
                    format!("MATCH (n:S) WHERE n.v IN {lit} RETURN n.id AS id"),
                    member(all),
                ),
            ] {
                assert_eq!(
                    ids(&db, &q, &lv).await,
                    want,
                    "flush={flush}: {q}, $b = {lv:?}"
                );
            }
        }
    }
}

/// `ORDER BY`, `min` and `max` over a column of mixed integers and floats
/// order by exact value.
#[tokio::test]
async fn ordering_and_extremes_are_exact() {
    for flush in [false, true] {
        let db = setup(flush).await;
        let all: Vec<(i64, Num)> = int_nums().into_iter().chain(float_nums()).collect();
        // Orderability: numbers ascending, NaN after +inf.
        let rank = |a: Num, b: Num| -> Ordering {
            let nan = |n: Num| matches!(n, Num::F(f) if f.is_nan());
            match (nan(a), nan(b)) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                _ => exact_cmp(a, b).unwrap(),
            }
        };
        for desc in [false, true] {
            let q = format!(
                "MATCH (n:S) RETURN n.id AS id ORDER BY n.v{}",
                if desc { " DESC" } else { "" }
            );
            let res = db.session().query(&q).await.unwrap();
            let got: Vec<Num> = res
                .rows()
                .iter()
                .map(|r| {
                    let id = r.get::<i64>("id").unwrap();
                    all.iter().find(|(i, _)| *i == id).unwrap().1
                })
                .collect();
            assert_eq!(got.len(), all.len());
            for w in got.windows(2) {
                let o = rank(w[0], w[1]);
                let ok = if desc {
                    o != Ordering::Less
                } else {
                    o != Ordering::Greater
                };
                assert!(ok, "flush={flush}: {q}: {:?} before {:?}", w[0], w[1]);
            }
        }
        let s = db.session();
        let one = |q: &'static str| {
            let s = s.clone();
            async move {
                let r = s.query(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
                r.rows()[0].value("r").cloned().unwrap()
            }
        };
        // Below 2^63 the largest value is the integer i64::MAX, one less than
        // 2^63, which widening would make equal to it.
        assert_eq!(
            one("MATCH (n:S) WHERE n.v <= 9223372036854775807 RETURN max(n.v) AS r").await,
            Value::Int(i64::MAX),
            "flush={flush}"
        );
        // -2^63 as a float equals i64::MIN; either is the minimum.
        let min = one("MATCH (n:S) WHERE n.v >= -9223372036854775808 RETURN min(n.v) AS r").await;
        assert!(
            matches!(min, Value::Int(i64::MIN)) || min == Value::Float(-TWO_63),
            "flush={flush}: {min:?}"
        );
    }
    let db = Uni::in_memory().build().await.unwrap();
    let s = db.session();
    for (q, want) in [
        (
            "UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN max(x) AS r",
            Value::Int(TWO_53 + 1),
        ),
        (
            "UNWIND [9007199254740993, 9007199254740994.0] AS x RETURN max(x) AS r",
            Value::Float((TWO_53 + 2) as f64),
        ),
        (
            "UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN min(x) AS r",
            Value::Float(TWO_53 as f64),
        ),
        (
            "UNWIND [9007199254740993, 9007199254740992.0, 9007199254740991] AS x \
             WITH x ORDER BY x RETURN collect(x) AS r",
            Value::List(vec![
                Value::Int(TWO_53 - 1),
                Value::Float(TWO_53 as f64),
                Value::Int(TWO_53 + 1),
            ]),
        ),
        (
            "UNWIND [9007199254740993, 9007199254740992.0, 9007199254740991] AS x \
             WITH x ORDER BY x DESC RETURN collect(x) AS r",
            Value::List(vec![
                Value::Int(TWO_53 + 1),
                Value::Float(TWO_53 as f64),
                Value::Int(TWO_53 - 1),
            ]),
        ),
    ] {
        let r = s.query(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        assert_eq!(r.rows()[0].value("r").unwrap(), &want, "{q}");
    }
}

/// `1` and `1.0` are one value: one row, one group, one list element. The
/// first value seen is the one kept. `2^53` and `2^53 + 1` stay apart from
/// each other while `2^53` and `2^53` as a float merge.
#[tokio::test]
async fn equal_integers_and_floats_are_one_value() {
    let db = Uni::in_memory().build().await.unwrap();
    let s = db.session();
    let rows = |q: &'static str| {
        let s = s.clone();
        async move {
            s.query(q)
                .await
                .unwrap_or_else(|e| panic!("{q}: {e}"))
                .rows()
                .iter()
                .map(|r| r.values().to_vec())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        rows("UNWIND [1, 1.0] AS x RETURN DISTINCT x").await,
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows("UNWIND [1.0, 1] AS x RETURN DISTINCT x").await,
        vec![vec![Value::Float(1.0)]]
    );
    assert_eq!(
        rows("UNWIND [1, 1.0, 2] AS x RETURN count(DISTINCT x) AS c").await,
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(
        rows("UNWIND [1, 1.0, 2] AS x RETURN collect(DISTINCT x) AS c").await,
        vec![vec![Value::List(vec![Value::Int(1), Value::Int(2)])]]
    );
    assert_eq!(
        rows("UNWIND [1, 1.0, 2] AS x RETURN x, count(*) AS c ORDER BY c DESC").await,
        vec![
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::Int(2), Value::Int(1)]
        ]
    );
    assert_eq!(
        rows("RETURN 1 AS x UNION RETURN 1.0 AS x").await,
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            "UNWIND [9007199254740992, 9007199254740993, 9007199254740992.0] AS x \
             RETURN count(DISTINCT x) AS c"
        )
        .await,
        vec![vec![Value::Int(2)]]
    );
    // `-0.0`, `0.0` and `0` are one value; NaN is one value.
    assert_eq!(
        rows("UNWIND [0, -0.0, 0.0, 0.0 / 0.0, 0.0 / 0.0] AS x RETURN count(DISTINCT x) AS c")
            .await,
        vec![vec![Value::Int(2)]]
    );

    // The same over stored properties, before and after a flush.
    for flush in [false, true] {
        let db = setup(flush).await;
        let s = db.session();
        let distinct = s
            .query("MATCH (n:S) RETURN count(DISTINCT n.v) AS c")
            .await
            .unwrap();
        let mut reps: Vec<Num> = Vec::new();
        for (_, v) in int_nums().into_iter().chain(float_nums()) {
            let same = |r: &Num| {
                holds(*r, "=", v)
                    || matches!((r, v), (Num::F(a), Num::F(b)) if a.is_nan() && b.is_nan())
            };
            if !reps.iter().any(same) {
                reps.push(v);
            }
        }
        let classes = reps.len() as i64;
        // 2^53, 2^53 + 2, 0 (twice), 1 and i64::MIN each merge with a float.
        assert_eq!(classes, 18);
        assert_eq!(
            distinct.rows()[0].get::<i64>("c").unwrap(),
            classes,
            "flush={flush}"
        );
        let grouped = s
            .query("MATCH (n:S) RETURN n.v AS v, count(*) AS c")
            .await
            .unwrap();
        assert_eq!(
            grouped.rows().len() as i64,
            classes,
            "flush={flush}: grouping"
        );
        let by_id = s
            .query(
                "MATCH (n:S) WITH n ORDER BY n.id \
                 WITH DISTINCT n.v AS v RETURN count(*) AS c",
            )
            .await
            .unwrap();
        assert_eq!(
            by_id.rows()[0].get::<i64>("c").unwrap(),
            classes,
            "flush={flush}"
        );
        let union = s
            .query("MATCH (n:M) WHERE n.i IS NOT NULL RETURN n.i AS v UNION MATCH (n:M) WHERE n.f IS NOT NULL RETURN n.f AS v")
            .await
            .unwrap();
        assert_eq!(union.rows().len() as i64, classes, "flush={flush}: UNION");
    }
}

/// A list literal keeps each element's own type when an element is a float
/// expression.
#[tokio::test]
async fn list_of_int_and_float_expression_keeps_element_types() {
    for flush in [false, true] {
        let db = setup(flush).await;
        let r = db
            .session()
            .query("MATCH (n:M {id: 109}) RETURN [9007199254740993, n.f] AS l, [n.f, 1] AS m")
            .await
            .unwrap();
        assert_eq!(
            r.rows()[0].value("l").unwrap(),
            &Value::List(vec![Value::Int(TWO_53 + 1), Value::Float(1.0)]),
            "flush={flush}"
        );
        assert_eq!(
            r.rows()[0].value("m").unwrap(),
            &Value::List(vec![Value::Float(1.0), Value::Int(1)]),
            "flush={flush}"
        );
        let r = db
            .session()
            .query("MATCH (n:M {id: 2}) RETURN [n.i, 0.5] AS l")
            .await
            .unwrap();
        assert_eq!(
            r.rows()[0].value("l").unwrap(),
            &Value::List(vec![Value::Int(TWO_53), Value::Float(0.5)]),
            "flush={flush}"
        );
    }
}

/// Edge property predicates on a single hop and on variable-length paths,
/// and the target's inline map in a pattern predicate, over unflushed and
/// flushed data: `(:A {id})-[:R {w}]->(:B {id, w})`, one edge per value, `w` a FLOAT and `k` an INT, both declared and schemaless (the
/// schemaless walk is a different operator).
#[tokio::test]
async fn edge_property_predicates_are_exact() {
    for (flush, declared) in [(false, true), (true, true), (false, false), (true, false)] {
        let db = Uni::in_memory().config(cfg()).build().await.unwrap();
        if declared {
            db.schema()
                .label("A")
                .property("id", DataType::Int64)
                .label("B")
                .property("id", DataType::Int64)
                .edge_type("R", &["A"], &["B"])
                .property_nullable("w", DataType::Float64)
                .property_nullable("k", DataType::Int64)
                .apply()
                .await
                .unwrap();
        }
        let session = db.session();
        let tx = session.tx().await.unwrap();
        for (id, v) in float_rows() {
            tx.execute_with("CREATE (:A {id: $id})-[:R {w: $v}]->(:B {id: $id, w: $v})")
                .param("id", Value::Int(id))
                .param("v", Value::Float(v))
                .run()
                .await
                .unwrap();
        }
        for (id, v) in int_rows() {
            tx.execute_with("CREATE (:A {id: $id})-[:R {k: $v}]->(:B {id: $id, k: $v})")
                .param("id", Value::Int(id))
                .param("v", Value::Int(v))
                .run()
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        if flush {
            db.flush().await.unwrap();
        }
        let mut failures = Vec::new();
        for b in bounds() {
            let lit = b.literal();
            for (prop, rows) in [("w", float_nums()), ("k", int_nums())] {
                for op in OPS {
                    let want = want_ids(&rows, op, b);
                    for q in [
                        format!("MATCH (a:A)-[r:R]->() WHERE r.{prop} {op} $b RETURN a.id AS id"),
                        format!(
                            "MATCH (a:A)-[rs:R*1..2]->() WHERE all(e IN rs WHERE e.{prop} {op} $b) \
                             RETURN a.id AS id"
                        ),
                    ] {
                        let got = ids(&db, &q, &b.value()).await;
                        if got != want {
                            failures.push(format!(
                                "flush={flush} declared={declared}: {q}, $b = {b:?}: got {got:?}, want {want:?}"
                            ));
                        }
                    }
                }
                // Inline property maps are equality.
                let want = want_ids(&rows, "=", b);
                for q in [
                    format!("MATCH (a:A)-[:R {{{prop}: $b}}]->() RETURN a.id AS id"),
                    format!("MATCH (a:A)-[:R*1..2 {{{prop}: $b}}]->() RETURN a.id AS id"),
                    format!("MATCH (a:A)-[:R*1..2 {{{prop}: {lit}}}]->() RETURN a.id AS id"),
                    // The target's inline map, in a pattern predicate.
                    format!("MATCH (a:A) WHERE (a)-[:R]->(:B {{{prop}: $b}}) RETURN a.id AS id"),
                    format!("MATCH (a:A) WHERE (a)-[:R]->(:B {{{prop}: {lit}}}) RETURN a.id AS id"),
                ] {
                    let got = ids(&db, &q, &b.value()).await;
                    if got != want {
                        failures.push(format!(
                            "flush={flush} declared={declared}: {q}, $b = {b:?}: got {got:?}, want {want:?}"
                        ));
                    }
                }
            }
        }
        // A null in an inline map matches nothing, a missing property included.
        for q in [
            "MATCH (a:A) WHERE (a)-[:R]->(:B {nope: null}) RETURN a.id AS id",
            "MATCH (a:A) WHERE (a)-[:R]->(:B {w: null}) RETURN a.id AS id",
        ] {
            let got = ids(&db, q, &Value::Null).await;
            if !got.is_empty() {
                failures.push(format!(
                    "flush={flush} declared={declared}: {q}: got {got:?}"
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "{} failures:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}

/// An `IN` list holding another variable's property is not pushed into the
/// scan of the left-hand variable, which cannot evaluate it. Planning failed
/// with "No field named m" before, for any element type.
#[tokio::test]
async fn in_list_of_another_variables_property_stays_above_the_scan() {
    let db = setup(false).await;
    for (q, want) in [
        (
            "MATCH (n:M), (m:M) WHERE n.i IN [m.i] RETURN n.id AS a, m.id AS b",
            int_rows().len(),
        ),
        (
            "MATCH (n:M), (m:M) WHERE n.i = [m.i][0] RETURN n.id AS a, m.id AS b",
            int_rows().len(),
        ),
    ] {
        let r = db
            .session()
            .query(q)
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        assert_eq!(r.rows().len(), want, "{q}");
        assert!(
            r.rows()
                .iter()
                .all(|row| row.get::<i64>("a").unwrap() == row.get::<i64>("b").unwrap()),
            "{q}"
        );
    }
}

/// The rows of `q`, each as its values.
async fn value_rows(db: &Uni, q: &str) -> Vec<Vec<Value>> {
    db.session()
        .query(q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .rows()
        .iter()
        .map(|r| r.values().to_vec())
        .collect()
}

/// An aggregate over `DISTINCT` sees the first value of each class of equal
/// numbers, as `RETURN DISTINCT` keeps it. `sum(DISTINCT x)` over `[1.0, 1]`
/// summed the class key, the integer `1`, so a float input came back an
/// integer.
#[tokio::test]
async fn distinct_aggregates_see_the_first_value_seen() {
    let db = Uni::in_memory().config(cfg()).build().await.unwrap();
    for (list, first) in [
        ("[1.0, 1]", Value::Float(1.0)),
        ("[1, 1.0]", Value::Int(1)),
        ("[1.0, 1, 1.0]", Value::Float(1.0)),
    ] {
        for (agg, want) in [
            ("sum(DISTINCT x)", first.clone()),
            ("min(DISTINCT x)", first.clone()),
            ("max(DISTINCT x)", first.clone()),
            ("collect(DISTINCT x)", Value::List(vec![first.clone()])),
            ("count(DISTINCT x)", Value::Int(1)),
            ("avg(DISTINCT x)", Value::Float(1.0)),
        ] {
            let q = format!("UNWIND {list} AS x RETURN {agg} AS a");
            assert_eq!(value_rows(&db, &q).await, vec![vec![want]], "{q}");
        }
    }
    // Two classes; each keeps its first value.
    let q = "UNWIND [2.0, 1, 2, 1.0] AS x RETURN sum(DISTINCT x) AS a";
    assert_eq!(
        value_rows(&db, q).await,
        vec![vec![Value::Float(3.0)]],
        "{q}"
    );
    let q = "UNWIND [1, 2.0, 1.0, 2] AS x RETURN collect(DISTINCT x) AS a";
    assert_eq!(
        value_rows(&db, q).await,
        vec![vec![Value::List(vec![Value::Int(1), Value::Float(2.0)])]],
        "{q}"
    );

    // The same over a stored column of mixed type, before and after a flush,
    // with the first row by id holding the float.
    for flush in [false, true] {
        let db = Uni::in_memory().config(cfg()).build().await.unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (:D {id: 1, v: 1.0}), (:D {id: 2, v: 1}), (:D {id: 3, v: 1.0})")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        if flush {
            db.flush().await.unwrap();
        }
        for (agg, want) in [
            ("sum(DISTINCT v)", Value::Float(1.0)),
            ("collect(DISTINCT v)", Value::List(vec![Value::Float(1.0)])),
            ("count(DISTINCT v)", Value::Int(1)),
        ] {
            let q = format!("MATCH (n:D) WITH n.v AS v ORDER BY n.id RETURN {agg} AS a");
            assert_eq!(
                value_rows(&db, &q).await,
                vec![vec![want]],
                "flush={flush}: {q}"
            );
        }
    }
}

/// A float MERGE key of `2^63` is not the integer `i64::MAX`. The key was
/// folded to an integer by an `f <= i64::MAX as f64` check, which `2^63`
/// passes because `i64::MAX` widens to `2^63`, and the cast then saturated;
/// so MERGE matched the existing `i64::MAX` node instead of creating one.
#[tokio::test]
async fn merge_keys_are_exact_at_the_i64_boundary() {
    for flush in [false, true] {
        let db = Uni::in_memory().config(cfg()).build().await.unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (:K {k: 9223372036854775807}), (:K {k: -9223372036854775808})")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        if flush {
            db.flush().await.unwrap();
        }
        let tx = db.session().tx().await.unwrap();
        tx.execute(
            "MERGE (:K {k: 9223372036854775808.0}) \
             MERGE (:K {k: -9223372036854775808.0}) \
             MERGE (:K {k: 9007199254740992.0})",
        )
        .await
        .unwrap();
        tx.execute(
            "UNWIND [9223372036854775807, 9223372036854775808.0, \
                     9007199254740993, 9007199254740992] AS k \
             MERGE (:B {k: k})",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let q = "MATCH (n:K) RETURN n.k AS k ORDER BY k";
        assert_eq!(
            value_rows(&db, q).await,
            vec![
                // `-2^63` is exactly `i64::MIN`, so that MERGE matched.
                vec![Value::Int(i64::MIN)],
                vec![Value::Float(9_007_199_254_740_992.0)],
                vec![Value::Int(i64::MAX)],
                vec![Value::Float(TWO_63)],
            ],
            "flush={flush}: {q}"
        );
        let q = "MATCH (n:B) RETURN count(*) AS c";
        assert_eq!(
            value_rows(&db, q).await,
            vec![vec![Value::Int(4)]],
            "flush={flush}: {q}"
        );
    }
}

/// A standalone `CALL` of a registered procedure runs the whole statement on
/// the fallback executor, whose `DISTINCT`, `UNION`, `count(DISTINCT)` and
/// `min`/`max` had their own numeric keys and comparison: `2^63` folded into
/// `i64::MAX`, and `2^53 + 1` tied with `2^53.0`.
#[tokio::test]
async fn fallback_executor_compares_numbers_exactly() {
    use std::collections::HashMap;
    use uni_query::{ProcedureOutput, ProcedureValueType, RegisteredProcedure};

    let db = Uni::in_memory().config(cfg()).build().await.unwrap();
    let register = |name: &str, values: &[Value]| {
        db.procedure_registry().register(RegisteredProcedure {
            name: name.to_string(),
            params: Vec::new(),
            outputs: vec![ProcedureOutput {
                name: "v".to_string(),
                output_type: ProcedureValueType::Number,
            }],
            data: values
                .iter()
                .map(|v| HashMap::from([("v".to_string(), v.clone())]))
                .collect(),
        });
    };
    let big = Value::Int(TWO_53 + 1);
    let big_f = Value::Float(TWO_53 as f64);
    let max = Value::Int(i64::MAX);
    let max_f = Value::Float(TWO_63);
    register("test.n53", &[big.clone(), big_f.clone()]);
    register("test.n53r", &[big_f.clone(), big.clone()]);
    register("test.n63", &[max.clone(), max_f.clone()]);
    register("test.n63r", &[max_f.clone(), max.clone()]);
    register("test.one", &[Value::Float(1.0), Value::Int(1)]);

    for (p, lo, hi) in [
        ("test.n53", &big_f, &big),
        ("test.n53r", &big_f, &big),
        ("test.n63", &max, &max_f),
        ("test.n63r", &max, &max_f),
    ] {
        let q = format!("CALL {p}() YIELD v RETURN min(v) AS lo, max(v) AS hi");
        assert_eq!(
            value_rows(&db, &q).await,
            vec![vec![lo.clone(), hi.clone()]],
            "{q}"
        );
        let q = format!("CALL {p}() YIELD v RETURN count(DISTINCT v) AS c");
        assert_eq!(value_rows(&db, &q).await, vec![vec![Value::Int(2)]], "{q}");
        let q = format!("CALL {p}() YIELD v RETURN DISTINCT v");
        assert_eq!(value_rows(&db, &q).await.len(), 2, "{q}");
        let q = format!("CALL {p}() YIELD v RETURN v, count(*) AS c");
        assert_eq!(value_rows(&db, &q).await.len(), 2, "{q}");
        let q = format!("CALL {p}() YIELD v RETURN v UNION CALL {p}() YIELD v RETURN v");
        assert_eq!(value_rows(&db, &q).await.len(), 2, "{q}");
    }
    // Equal numbers are still one value there, and the first one is kept.
    let q = "CALL test.one() YIELD v RETURN DISTINCT v";
    assert_eq!(
        value_rows(&db, q).await,
        vec![vec![Value::Float(1.0)]],
        "{q}"
    );
    let q = "CALL test.one() YIELD v RETURN count(DISTINCT v) AS c";
    assert_eq!(value_rows(&db, q).await, vec![vec![Value::Int(1)]], "{q}");
    // Every aggregate honors DISTINCT there, not only `count`.
    for (agg, want) in [
        ("sum(DISTINCT v)", Value::Float(1.0)),
        ("collect(DISTINCT v)", Value::List(vec![Value::Float(1.0)])),
        ("avg(DISTINCT v)", Value::Float(1.0)),
        ("max(DISTINCT v)", Value::Float(1.0)),
    ] {
        let q = format!("CALL test.one() YIELD v RETURN {agg} AS a");
        assert_eq!(value_rows(&db, &q).await, vec![vec![want]], "{q}");
    }
}

/// A list of integers compares with a list of floats element by element, by
/// exact value. The two lists were cast to a list of floats first, so
/// `[9007199254740993] = [9007199254740992.0]` was true.
#[tokio::test]
async fn integer_and_float_lists_compare_exactly() {
    let db = Uni::in_memory().config(cfg()).build().await.unwrap();
    for (expr, want) in [
        ("[9007199254740993] = [9007199254740992.0]", false),
        ("[9007199254740993] <> [9007199254740992.0]", true),
        ("[9007199254740993] > [9007199254740992.0]", true),
        ("[9007199254740992.0] < [9007199254740993]", true),
        ("[9223372036854775807] < [9223372036854775808.0]", true),
        ("[1, 2] = [1.0, 2.0]", true),
        ("[2] > [1.5]", true),
        ("[1] < [1.5]", true),
    ] {
        for q in [
            format!("RETURN {expr} AS r"),
            format!("WITH {} RETURN {} AS r", "1 AS one", expr),
        ] {
            assert_eq!(
                value_rows(&db, &q).await,
                vec![vec![Value::Bool(want)]],
                "{q}"
            );
        }
    }
}
