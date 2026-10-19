// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cypher operators answer what openCypher specifies on both of Cypher's
//! expression paths: literals (static Arrow types and kernels) and
//! schemaless properties (CypherValue columns evaluated per row).
//!
//! Each test pins one class the operator-parity matrix
//! (`locy::locy_operator_parity`) exposed: a DataFusion panic on
//! `n.missing AND null`, planning errors on a NULL operand, `n.s =~ 'ab'`
//! failing to plan, `true OR n.i` answering by batch short-circuit, `^`
//! answering an integer, `date - date` a day count, `datetime + duration`
//! failing to plan, `n.s + 1` an error, and local datetimes compared as
//! instants.

use anyhow::Result;
use uni_db::{Uni, Value};

/// One node with a schemaless property of each type, so `n.x` is a
/// CypherValue column.
const GRAPH: &str = "CREATE (:One {i: 2, f: 2.5, s: 'ab', b: true, l: [1, 2], \
     d: date('2020-01-02'), dt: datetime('2020-01-02T03:04:05Z'), dur: duration('P1D'), \
     ldt: localdatetime('2020-01-02T03:04:05')})";

async fn graph() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(GRAPH).await?;
    tx.commit().await?;
    Ok(db)
}

/// The value of `expr` over the single node, or the error message.
async fn eval(db: &Uni, expr: &str) -> std::result::Result<Value, String> {
    let result = db
        .session()
        .query(&format!("MATCH (n:One) RETURN {expr} AS v"))
        .await
        .map_err(|e| e.to_string())?;
    match result.rows() {
        [row] => Ok(row.values()[0].clone()),
        rows => Err(format!("expected one row, got {}", rows.len())),
    }
}

/// Asserts each expression answers `want`.
async fn assert_values(db: &Uni, cases: &[(&str, Value)]) {
    for (expr, want) in cases {
        assert_eq!(eval(db, expr).await.as_ref(), Ok(want), "{expr}");
    }
}

/// Asserts each expression is an error.
async fn assert_errors(db: &Uni, exprs: &[&str]) {
    for expr in exprs {
        let got = eval(db, expr).await;
        assert!(got.is_err(), "{expr}: want an error, got {got:?}");
    }
}

fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

fn duration(months: i64, days: i64, nanos: i64) -> Value {
    Value::Temporal(uni_common::TemporalValue::Duration {
        months,
        days,
        nanos,
    })
}

#[tokio::test]
async fn logical_operator_over_null_is_null_not_a_panic() -> Result<()> {
    let db = graph().await?;
    // `n.missing AND null` panicked in DataFusion ("boolean_op failed to
    // downcast right array"); `null AND n.b` failed to plan.
    assert_values(
        &db,
        &[
            ("n.missing AND null", Value::Null),
            ("n.missing OR null", Value::Null),
            ("null AND n.b", Value::Null),
            ("null OR n.b", Value::Bool(true)),
            ("n.b AND null", Value::Null),
            ("null AND n.missing", Value::Null),
        ],
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn or_does_not_depend_on_which_rows_share_a_batch() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:R {k: 1, b: true, i: 1}), (:R {k: 2, b: true, i: 1}), (:R {k: 3, b: false, i: true})")
        .await?;
    tx.commit().await?;
    // DataFusion skips the right operand of OR only when the left one is
    // true on every row of the batch, so the rows with `b: true` answered
    // true alone and a type error beside the `b: false` row. Per row, a
    // `true` operand decides.
    for expr in ["n.b OR n.i", "n.i OR n.b"] {
        let result = db
            .session()
            .query(&format!("MATCH (n:R) RETURN {expr} AS v ORDER BY n.k"))
            .await
            .map_err(|e| anyhow::anyhow!("{expr}: {e}"))?;
        let got: Vec<Value> = result
            .rows()
            .iter()
            .map(|r| r.values()[0].clone())
            .collect();
        assert_eq!(got, vec![Value::Bool(true); 3], "{expr}");
    }
    Ok(())
}

#[tokio::test]
async fn kleene_logic_with_a_non_boolean_operand() -> Result<()> {
    let db = graph().await?;
    // A dominating operand decides, in either position; otherwise a
    // non-boolean operand is a type error, beside NULL too.
    assert_values(
        &db,
        &[
            ("true OR n.i", Value::Bool(true)),
            ("n.i OR true", Value::Bool(true)),
            ("false AND n.i", Value::Bool(false)),
            ("n.i AND false", Value::Bool(false)),
            ("true OR date('2020-01-02')", Value::Bool(true)),
        ],
    )
    .await;
    assert_errors(
        &db,
        &[
            "null AND n.i",
            "n.missing AND n.i",
            "n.missing XOR n.i",
            "true AND n.i",
            // A non-boolean literal is rejected at compile time (TCK Boolean1-3).
            "true OR 1",
            "null AND 1",
        ],
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn regex_over_a_schemaless_property() -> Result<()> {
    let db = graph().await?;
    // `n.s =~ 'ab'` failed to plan: "Cannot infer common argument type for
    // regex operation LargeBinary ~ Utf8".
    assert_values(
        &db,
        &[
            ("n.s =~ 'ab'", Value::Bool(true)),
            ("'ab' =~ n.s", Value::Bool(true)),
            ("n.s =~ 'x.*'", Value::Bool(false)),
            ("n.s =~ null", Value::Null),
            ("n.i =~ null", Value::Null),
            ("null =~ n.s", Value::Null),
            ("n.missing =~ 'ab'", Value::Null),
        ],
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn power_is_a_float_and_propagates_null() -> Result<()> {
    let db = graph().await?;
    assert_values(
        &db,
        &[
            ("2 ^ 3", Value::Float(8.0)),
            ("n.i ^ 3", Value::Float(8.0)),
            ("n.f ^ 2", Value::Float(6.25)),
            // `power(cypher_number('ab'), ..)` failed decoding 'ab'.
            ("'ab' ^ null", Value::Null),
            ("n.s ^ n.missing", Value::Null),
            ("[1, 2] ^ null", Value::Null),
        ],
    )
    .await;
    assert_errors(&db, &["n.s ^ 2", "'ab' ^ 2"]).await;
    Ok(())
}

#[tokio::test]
async fn arithmetic_over_a_null_literal_is_null() -> Result<()> {
    let db = graph().await?;
    // Each failed to plan ("Cannot coerce arithmetic expression Utf8 +
    // Utf8", "Cannot get result type for temporal operation").
    let cases: Vec<(&str, Value)> = [
        "'ab' + null",
        "null - 'ab'",
        "true * null",
        "[1, 2] - null",
        "{k: 1} / null",
        "date('2020-01-02') + null",
        "datetime('2020-01-02T03:04:05Z') % null",
        "duration('P1D') * null",
        "null / duration('P1D')",
    ]
    .into_iter()
    .map(|e| (e, Value::Null))
    .collect();
    assert_values(&db, &cases).await;
    Ok(())
}

#[tokio::test]
async fn temporal_arithmetic_agrees_between_literals_and_properties() -> Result<()> {
    let db = graph().await?;
    // `date - date` was an integer day count on the literal path;
    // `datetime + duration` and `duration * 2` failed to plan there.
    let pairs = [
        (
            "date('2020-01-02') - date('2020-01-02')",
            "n.d - date('2020-01-02')",
        ),
        (
            "datetime('2020-01-02T03:04:05Z') + duration('PT1H')",
            "n.dt + duration('PT1H')",
        ),
        (
            "datetime('2020-01-02T03:04:05Z') - datetime('2020-01-02T03:04:05Z')",
            "n.dt - datetime('2020-01-02T03:04:05Z')",
        ),
        ("duration('P1D') * 2", "n.dur * 2"),
        ("2.5 * duration('P1D')", "2.5 * n.dur"),
        ("duration('P1D') / 2", "n.dur / 2"),
    ];
    for (literal, property) in pairs {
        let (l, p) = (eval(&db, literal).await, eval(&db, property).await);
        assert!(l.is_ok(), "{literal}: {l:?}");
        assert_eq!(l, p, "{literal} vs {property}");
    }
    assert_eq!(
        eval(&db, "date('2020-01-02') - date('2020-01-02')").await,
        Ok(duration(0, 0, 0))
    );
    assert_eq!(
        eval(&db, "duration('P1D') * 2").await,
        Ok(duration(0, 2, 0))
    );
    // A number is not a duration (the row path read it as microseconds).
    assert_errors(
        &db,
        &[
            "datetime('2020-01-02T03:04:05Z') + 1",
            "n.dt + 1",
            "date('2020-01-02') - 1",
            "n.d - 1",
            "1 + n.d",
        ],
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn string_plus_scalar_concatenates_on_both_paths() -> Result<()> {
    let db = graph().await?;
    // The row path errored ("Invalid types for addition"); the literal path
    // rendered a duration as Arrow's `1 days` and a float through Arrow's
    // cast.
    assert_values(
        &db,
        &[
            ("n.s + 1", s("ab1")),
            ("1 + n.s", s("1ab")),
            ("n.s + 1.0", s("ab1.0")),
            ("'ab' + 1.0", s("ab1.0")),
            ("n.s + true", s("abtrue")),
            ("n.s + n.dur", s("abP1D")),
            ("'ab' + duration('P1D')", s("abP1D")),
            ("n.d + 'x'", s("2020-01-02x")),
            (
                "'x' + datetime('2020-01-02T03:04:05Z')",
                s("x2020-01-02T03:04:05Z"),
            ),
        ],
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn non_numeric_property_against_an_integer_column_is_not_null() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL T (k INT)").await?;
    tx.execute("CREATE (:T {k: 1, s: 'ab', dur: duration('P1D')}), (:T {k: 2, s: 'cd', dur: duration('P1D')})")
        .await?;
    tx.commit().await?;
    // A schemaless (CypherValue) column against a declared Int64 column takes
    // a vectorized fast path, which answered NULL for a non-numeric value
    // instead of handing the batch to the evaluator: `n.dur * n.k` was NULL,
    // not a duration, and `n.s - n.k` NULL, not a type error.
    let result = db
        .session()
        .query("MATCH (n:T) RETURN n.dur * n.k AS v, n.k * n.dur AS w ORDER BY n.k")
        .await?;
    let got: Vec<(Value, Value)> = result
        .rows()
        .iter()
        .map(|r| (r.values()[0].clone(), r.values()[1].clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            (duration(0, 1, 0), duration(0, 1, 0)),
            (duration(0, 2, 0), duration(0, 2, 0))
        ]
    );
    let err = db
        .session()
        .query("MATCH (n:T) RETURN n.s - n.k AS v")
        .await;
    assert!(err.is_err(), "n.s - n.k: want a type error, got {err:?}");
    Ok(())
}

#[tokio::test]
async fn local_datetime_is_not_comparable_with_a_zoned_datetime_or_string() -> Result<()> {
    let db = graph().await?;
    // Literals compared the instants (`=` true) and cast the string to a
    // timestamp (a parse error on 'ab'); the row path answers both as
    // different types.
    assert_values(
        &db,
        &[
            (
                "localdatetime('2020-01-02T03:04:05') = datetime('2020-01-02T03:04:05Z')",
                Value::Bool(false),
            ),
            (
                "localdatetime('2020-01-02T03:04:05') < datetime('2020-01-02T03:04:05Z')",
                Value::Null,
            ),
            (
                "n.ldt = datetime('2020-01-02T03:04:05Z')",
                Value::Bool(false),
            ),
            (
                "'ab' = localdatetime('2020-01-02T03:04:05')",
                Value::Bool(false),
            ),
            (
                "'ab' <> localdatetime('2020-01-02T03:04:05')",
                Value::Bool(true),
            ),
            ("'ab' < localdatetime('2020-01-02T03:04:05')", Value::Null),
            ("null = localdatetime('2020-01-02T03:04:05')", Value::Null),
        ],
    )
    .await;
    // Negating a local datetime negated its epoch offset.
    assert_errors(&db, &["- localdatetime('2020-01-02T03:04:05')", "- n.ldt"]).await;
    Ok(())
}
