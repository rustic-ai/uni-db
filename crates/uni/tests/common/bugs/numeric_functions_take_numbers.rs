// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Numeric functions and statistics take numbers, and reject anything else.
//!
//! As in Neo4j, `ceil('x')` or `sqrt` of a duration is a type error, and so
//! is `stDev` or `percentileCont` over strings. The DataFusion path converted
//! its argument through `_cypher_to_float64`, which turns a non-number into
//! NULL, so these answered NULL while `abs('x')` and the row evaluator's math
//! functions failed. Statistics over a typed string column cast it, so `'12'`
//! counted as 12. All now read through `_cypher_number`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(numeric_functions_take_numbers)'

// Rust guideline compliant

use anyhow::{Result, bail};
use uni_db::{DataType, Uni, Value};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("V")
        .property("s", DataType::String)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:P {s: 'x', d: duration('P1D'), n: -2.5}), (:P {s: 'y', d: duration('P2D'), n: 4}), \
         (:V {s: '12'}), (:V {s: '3'})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn values(db: &Uni, query: &str) -> Result<Vec<Value>> {
    let result = db.session().query(query).await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| r.values()[0].clone())
        .collect())
}

#[tokio::test]
async fn numeric_functions_take_numbers() -> Result<()> {
    let db = open().await?;
    let mut failures = Vec::new();
    for query in [
        "RETURN ceil('x')",
        "RETURN sqrt('x')",
        "RETURN 'x' ^ 2",
        "MATCH (p:P) RETURN ceil(p.s)",
        "MATCH (p:P) RETURN floor(p.s)",
        "MATCH (p:P) RETURN round(p.s)",
        "MATCH (p:P) RETURN sign(p.s)",
        "MATCH (p:P) RETURN exp(p.s)",
        "MATCH (p:P) RETURN sqrt(p.d)",
        "MATCH (p:P) RETURN log(p.d)",
        "MATCH (p:P) RETURN atan2(p.s, 1)",
        "MATCH (p:P) RETURN p.d ^ 2",
        "MATCH (p:P) RETURN stDev(p.s)",
        "MATCH (p:P) RETURN variance(p.d)",
        "MATCH (p:P) RETURN percentileCont(p.s, 0.5)",
        "MATCH (p:P) RETURN percentileDisc(p.d, 0.5)",
        "MATCH (v:V) RETURN stDev(v.s)",
        "MATCH (v:V) RETURN percentileCont(v.s, 0.5)",
    ] {
        if let Ok(got) = values(&db, query).await {
            failures.push(format!("{query}: expected a type error, got {got:?}"));
        }
    }
    // Numbers, schemaless or not, and NULL are unchanged.
    for (query, want) in [
        (
            "MATCH (p:P) RETURN ceil(p.n) ORDER BY p.n",
            vec![Value::Float(-2.0), Value::Float(4.0)],
        ),
        (
            "MATCH (p:P) RETURN sign(p.n) ORDER BY p.n",
            vec![Value::Int(-1), Value::Int(1)],
        ),
        ("RETURN ceil(null)", vec![Value::Null]),
        ("RETURN 2 ^ 3", vec![Value::Float(8.0)]),
        // percentileDisc answers the input itself, the integer 4.
        (
            "MATCH (p:P) RETURN percentileDisc(p.n, 1.0)",
            vec![Value::Int(4)],
        ),
        (
            "UNWIND [1, 3] AS x RETURN stDev(x)",
            vec![Value::Float(2.0_f64.sqrt())],
        ),
    ] {
        match values(&db, query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}: expected {want:?}, got {got:?}")),
            Err(e) => failures.push(format!("{query}: expected {want:?}, got error {e}")),
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}

/// `pow` / `power` take integers as `^` does, and `uni.bitwise.shiftLeft` /
/// `shiftRight` are callable by their documented camel-case names. A shift by
/// an amount outside `0..=63` has no answer and is an error.
#[tokio::test]
async fn pow_and_bitwise_shifts_are_callable() -> Result<()> {
    let db = open().await?;
    let mut failures = Vec::new();
    for (query, want) in [
        ("RETURN pow(2, 3)", vec![Value::Float(8.0)]),
        ("RETURN power(2, 10)", vec![Value::Float(1024.0)]),
        ("RETURN pow(2.0, 0.5)", vec![Value::Float(2.0_f64.sqrt())]),
        ("RETURN pow(null, 2)", vec![Value::Null]),
        (
            "MATCH (p:P) RETURN pow(p.n, 2) ORDER BY p.n",
            vec![Value::Float(6.25), Value::Float(16.0)],
        ),
        (
            "UNWIND [1, 2] AS x RETURN pow(x, 2) = x ^ 2",
            vec![Value::Bool(true), Value::Bool(true)],
        ),
        ("RETURN uni.bitwise.shiftLeft(1, 4)", vec![Value::Int(16)]),
        (
            "RETURN uni.bitwise.shiftRight(256, 4)",
            vec![Value::Int(16)],
        ),
        (
            "RETURN uni.bitwise.shiftRight(-16, 2)",
            vec![Value::Int(-4)],
        ),
        (
            "UNWIND [1, 2, 3] AS x RETURN uni.bitwise.shiftLeft(x, 2)",
            vec![Value::Int(4), Value::Int(8), Value::Int(12)],
        ),
        (
            "UNWIND [0, 1, 63] AS k RETURN uni.bitwise.shiftRight(uni.bitwise.shiftLeft(1, k), k)",
            vec![Value::Int(1), Value::Int(1), Value::Int(-1)],
        ),
        ("RETURN uni.bitwise.or(12, 3)", vec![Value::Int(15)]),
    ] {
        match values(&db, query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}: expected {want:?}, got {got:?}")),
            Err(e) => failures.push(format!("{query}: expected {want:?}, got error {e}")),
        }
    }
    for query in [
        "RETURN uni.bitwise.shiftLeft(1, 64)",
        "RETURN uni.bitwise.shiftLeft(1, -1)",
        "UNWIND [1, 64] AS k RETURN uni.bitwise.shiftRight(8, k)",
        "RETURN pow('x', 2)",
    ] {
        if let Ok(got) = values(&db, query).await {
            failures.push(format!("{query}: expected an error, got {got:?}"));
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}
