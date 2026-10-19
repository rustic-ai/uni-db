// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `percentileDisc` returns one of its inputs, exactly, and `percentileCont`
//! interpolates between exact inputs; both read their percentile argument.
//!
//! openCypher's `percentileDisc` is a discrete percentile: the answer is an
//! actual input value, so an integer input gives an integer and `2^53 + 1`
//! stays `2^53 + 1`. The DataFusion path read every input through `f64`, so
//! `percentileDisc` over integers answered a float and `2^53 + 1` came back
//! `9007199254740992.0`. The fallback executor (a standalone `CALL` of a
//! registered procedure runs the whole statement there) never evaluated the
//! percentile argument at all and always answered the 0th percentile.
//!
//! The oracle sorts the inputs by exact value and picks index
//! `round(p * (n - 1))`, the rule both executors use. Every check runs on the
//! DataFusion path (an `UNWIND`, a typed column, a schemaless column of mixed
//! type, in L0 and after a flush) and on the fallback executor, and the two
//! must agree.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(percentile_exact)'

// Rust guideline compliant

use std::cmp::Ordering;
use std::collections::HashMap;

use uni_db::{DataType, Uni, UniConfig, Value};
use uni_query::{ProcedureOutput, ProcedureValueType, RegisteredProcedure};

const TWO_53: i64 = 1 << 53;

const PERCENTILES: [f64; 7] = [0.0, 0.2, 0.25, 0.5, 0.75, 0.8, 1.0];

fn cfg() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

/// The input sets. No set holds an integer and a float of equal value, so
/// which of two equal inputs comes first never decides an answer.
fn sets() -> Vec<(&'static str, Vec<Value>)> {
    vec![
        (
            "ints",
            vec![
                Value::Int(3),
                Value::Int(TWO_53 + 1),
                Value::Int(1),
                Value::Int(2),
            ],
        ),
        (
            "floats",
            vec![Value::Float(30.0), Value::Float(10.0), Value::Float(20.0)],
        ),
        (
            "mixed",
            vec![
                Value::Int(TWO_53 + 1),
                Value::Float(2.5),
                Value::Int(-3),
                Value::Float(TWO_53 as f64),
                Value::Int(1),
            ],
        ),
        ("big", vec![Value::Int(TWO_53 + 5), Value::Int(TWO_53 + 1)]),
    ]
}

/// Exact order of two numbers: an `i64` against an `f64` by the float's
/// integral part and then its fraction, never by widening the integer.
fn exact_cmp(a: &Value, b: &Value) -> Ordering {
    let int_float = |i: i64, f: f64| -> Ordering {
        let ff = f.floor();
        if ff >= 9_223_372_036_854_775_808.0 {
            return Ordering::Less;
        }
        if ff < -9_223_372_036_854_775_808.0 {
            return Ordering::Greater;
        }
        match i.cmp(&(ff as i64)) {
            Ordering::Equal if f > ff => Ordering::Less,
            other => other,
        }
    };
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap(),
        (Value::Int(x), Value::Float(y)) => int_float(*x, *y),
        (Value::Float(x), Value::Int(y)) => int_float(*y, *x).reverse(),
        _ => panic!("not numbers: {a:?} {b:?}"),
    }
}

fn want_disc(values: &[Value], p: f64) -> Value {
    let mut sorted = values.to_vec();
    sorted.sort_by(exact_cmp);
    let idx = (p * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx].clone()
}

/// The same variant and the same bits: `Value`'s equality is enough for
/// that, except that it would not tell `Int(3)` from `Float(3.0)` if it ever
/// widened, so the variant is checked first.
fn identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x.to_bits() == y.to_bits(),
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

async fn one(db: &Uni, q: &str, p: f64) -> Result<Value, String> {
    let rows = db
        .session()
        .query_with(q)
        .param("p", Value::Float(p))
        .fetch_all()
        .await
        .map_err(|e| e.to_string())?;
    let rows = rows.rows();
    assert_eq!(rows.len(), 1, "{q} with $p = {p}");
    Ok(rows[0].values()[0].clone())
}

fn list_literal(values: &[Value]) -> String {
    let items: Vec<String> = values
        .iter()
        .map(|v| match v {
            Value::Int(i) => i.to_string(),
            Value::Float(f) => format!("{f:?}"),
            other => panic!("{other:?}"),
        })
        .collect();
    format!("[{}]", items.join(", "))
}

fn register(db: &Uni, name: &str, values: &[Value]) {
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
}

/// `:I {iv}` typed `Int64`, `:F {fv}` typed `Float64` and the schemaless
/// `:S {set, v}` holding every set, so `S.v` is one column of mixed type.
async fn setup(flush: bool) -> Uni {
    let db = Uni::in_memory().config(cfg()).build().await.unwrap();
    db.schema()
        .label("I")
        .property("iv", DataType::Int64)
        .done()
        .label("F")
        .property("fv", DataType::Float64)
        .done()
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    for (name, values) in sets() {
        for v in &values {
            tx.execute_with("CREATE (:S {set: $set, v: $v})")
                .param("set", Value::String(name.to_string()))
                .param("v", v.clone())
                .run()
                .await
                .unwrap();
        }
    }
    for (label, prop, set) in [("I", "iv", "ints"), ("F", "fv", "floats")] {
        let (_, values) = sets().into_iter().find(|(n, _)| *n == set).unwrap();
        for v in values {
            tx.execute_with(&format!("CREATE (:{label} {{{prop}: $v}})"))
                .param("v", v)
                .run()
                .await
                .unwrap();
        }
    }
    tx.commit().await.unwrap();
    if flush {
        db.flush().await.unwrap();
    }
    db
}

/// `percentileDisc` answers an actual input, with its type and its exact
/// value, on every path; the fallback executor honors the percentile.
#[tokio::test]
async fn percentile_exact_disc_returns_an_input() {
    let mut failures = Vec::new();
    for flush in [false, true] {
        let db = setup(flush).await;
        for (name, values) in sets() {
            register(&db, &format!("test.pct_{name}"), &values);
            let mut queries = vec![
                format!(
                    "UNWIND {} AS v RETURN percentileDisc(v, $p) AS r",
                    list_literal(&values)
                ),
                format!("MATCH (s:S {{set: '{name}'}}) RETURN percentileDisc(s.v, $p) AS r"),
                format!("CALL test.pct_{name}() YIELD v RETURN percentileDisc(v, $p) AS r"),
            ];
            match name {
                "ints" => queries.push("MATCH (n:I) RETURN percentileDisc(n.iv, $p) AS r".into()),
                "floats" => queries.push("MATCH (n:F) RETURN percentileDisc(n.fv, $p) AS r".into()),
                _ => {}
            }
            for p in PERCENTILES {
                let want = want_disc(&values, p);
                for q in &queries {
                    match one(&db, q, p).await {
                        Ok(got) if identical(&got, &want) => {}
                        Ok(got) => failures.push(format!(
                            "flush={flush} {q} with $p = {p}: want {want:?}, got {got:?}"
                        )),
                        Err(e) => failures.push(format!(
                            "flush={flush} {q} with $p = {p}: want {want:?}, got error {e}"
                        )),
                    }
                }
            }
        }
        // Grouped: each group sees its own inputs.
        let q = "MATCH (s:S) RETURN s.set AS grp, percentileDisc(s.v, 1.0) AS r ORDER BY grp";
        let rows = db.session().query(q).await.unwrap();
        for row in rows.rows() {
            let set: String = row.get("grp").unwrap();
            let (_, values) = sets().into_iter().find(|(n, _)| *n == set).unwrap();
            let want = want_disc(&values, 1.0);
            let got = row.values()[1].clone();
            if !identical(&got, &want) {
                failures.push(format!(
                    "flush={flush} {q}: {set} want {want:?}, got {got:?}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// `percentileCont` answers a float, interpolated between the exact inputs:
/// halfway between `2^53 + 1` and `2^53 + 5` is `2^53 + 3`, which rounds to
/// `2^53 + 4`. Read through `f64` first, the inputs were `2^53` and
/// `2^53 + 4`, and the answer `2^53 + 2`.
#[tokio::test]
async fn percentile_exact_cont_interpolates_exact_inputs() {
    let db = setup(false).await;
    let (_, big) = sets().into_iter().find(|(n, _)| *n == "big").unwrap();
    register(&db, "test.pct_big", &big);
    let (_, floats) = sets().into_iter().find(|(n, _)| *n == "floats").unwrap();
    register(&db, "test.pct_floats", &floats);
    let mut failures = Vec::new();
    let cases: Vec<(&str, f64, f64)> = vec![
        ("big", 0.0, (TWO_53 + 1) as f64),
        ("big", 0.25, (TWO_53 + 2) as f64),
        ("big", 0.5, (TWO_53 + 4) as f64),
        ("big", 0.75, (TWO_53 + 4) as f64),
        ("big", 1.0, (TWO_53 + 5) as f64),
        ("floats", 0.0, 10.0),
        ("floats", 0.25, 15.0),
        ("floats", 0.5, 20.0),
        ("floats", 1.0, 30.0),
    ];
    for (name, p, want) in cases {
        let values = if name == "big" { &big } else { &floats };
        for q in [
            format!(
                "UNWIND {} AS v RETURN percentileCont(v, $p) AS r",
                list_literal(values)
            ),
            format!("MATCH (s:S {{set: '{name}'}}) RETURN percentileCont(s.v, $p) AS r"),
            format!("CALL test.pct_{name}() YIELD v RETURN percentileCont(v, $p) AS r"),
        ] {
            match one(&db, &q, p).await {
                Ok(got) if identical(&got, &Value::Float(want)) => {}
                Ok(got) => failures.push(format!("{q} with $p = {p}: want {want:?}, got {got:?}")),
                Err(e) => failures.push(format!("{q} with $p = {p}: want {want:?}, got error {e}")),
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A percentile outside `[0, 1]`, or one that is not a number, is an error on
/// both paths, and so is an input that is not a number. The fallback
/// executor never read the percentile, so it answered there.
#[tokio::test]
async fn percentile_exact_rejects_bad_arguments() {
    let db = setup(false).await;
    register(&db, "test.pct_ints", &sets()[0].1);
    let mut failures = Vec::new();
    for f in ["percentileDisc", "percentileCont"] {
        for p in ["1.1", "-1", "1000", "'x'"] {
            for q in [
                format!("UNWIND [1, 2, 3] AS v RETURN {f}(v, {p}) AS r"),
                format!("MATCH (n:I) RETURN {f}(n.iv, {p}) AS r"),
                format!("CALL test.pct_ints() YIELD v RETURN {f}(v, {p}) AS r"),
            ] {
                if let Ok(got) = one(&db, &q, 0.0).await {
                    failures.push(format!("{q}: expected an error, got {got:?}"));
                }
            }
        }
        for q in [
            format!("UNWIND [1, 'a'] AS v RETURN {f}(v, 0.5) AS r"),
            format!("UNWIND ['a', 'b'] AS v RETURN {f}(v, 0.5) AS r"),
        ] {
            if let Ok(got) = one(&db, &q, 0.0).await {
                failures.push(format!("{q}: expected an error, got {got:?}"));
            }
        }
    }
    // NULL inputs are skipped, and no input at all answers NULL.
    for (q, want) in [
        (
            "UNWIND [null, 2, null, 7] AS v RETURN percentileDisc(v, 1.0) AS r",
            Value::Int(7),
        ),
        (
            "UNWIND [null] AS v RETURN percentileDisc(v, 0.5) AS r",
            Value::Null,
        ),
        (
            "MATCH (n:Nothing) RETURN percentileDisc(n.v, 0.5) AS r",
            Value::Null,
        ),
    ] {
        match one(&db, q, 0.0).await {
            Ok(got) if identical(&got, &want) => {}
            Ok(got) => failures.push(format!("{q}: want {want:?}, got {got:?}")),
            Err(e) => failures.push(format!("{q}: want {want:?}, got error {e}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
