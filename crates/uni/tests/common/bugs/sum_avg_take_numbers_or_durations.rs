// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `sum` and `avg` take numbers or durations, and reject anything else.
//!
//! As in Neo4j, `sum` of durations is their total and `avg` their mean, and
//! a string, a boolean, or numbers mixed with durations are a type error.
//! uni-db's Cypher-value `sum` skipped every value that was not a number, so
//! `sum` over durations or strings answered 0, and `avg` (through a float
//! coercion) answered NULL. A typed string column was cast to an integer, so
//! `sum` over `'12'` answered 12. Locy's `QUERY ... RETURN` raised a type
//! error for durations. All now share one accumulator
//! (`uni_query_functions::cypher_sum`), and so does a window `sum` / `avg`,
//! which summed strings and failed on durations and schemaless properties.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(sum_avg_take_numbers_or_durations)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Result, bail};
use uni_common::value::TemporalValue;
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{DataType, Uni, Value};

const HOUR_NANOS: i64 = 3_600_000_000_000;

fn duration(days: i64, nanos: i64) -> Value {
    Value::Temporal(TemporalValue::Duration {
        months: 0,
        days,
        nanos,
    })
}

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("T")
        .property_nullable("d", DataType::Duration)
        .property_nullable("s", DataType::String)
        .done()
        .label("V")
        .property("s", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:T {d: duration('P1D'), s: '12'}), (:T {d: duration('PT2H'), s: 'x'}), \
         (:T), (:U {d: duration('P1D'), s: 'a'}), (:U {d: duration('PT2H')}), (:V {s: '12'}), (:V {s: '3'})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn one(db: &Uni, query: &str) -> Result<Value> {
    let result = db.session().query(query).await?;
    let [row] = result.rows() else {
        bail!("{query}: expected one row, got {:?}", result.rows());
    };
    Ok(row.values()[0].clone())
}

#[tokio::test]
async fn sum_avg_take_numbers_or_durations_in_cypher() -> Result<()> {
    let db = open().await?;
    let total = duration(1, 2 * HOUR_NANOS);
    let mean = duration(0, 13 * HOUR_NANOS);
    let cases = [
        // A typed duration column, a schemaless one, and a list.
        ("MATCH (t:T) RETURN sum(t.d)", total.clone()),
        ("MATCH (t:T) RETURN avg(t.d)", mean.clone()),
        ("MATCH (u:U) RETURN sum(u.d)", total.clone()),
        ("MATCH (u:U) RETURN avg(u.d)", mean.clone()),
        (
            "UNWIND [duration('P1D'), null, duration('PT2H')] AS d RETURN sum(d)",
            total.clone(),
        ),
        (
            "UNWIND [duration('P1D'), duration('PT2H')] AS d RETURN avg(d)",
            mean,
        ),
        (
            "UNWIND [duration('P1D'), duration('P1D'), duration('PT2H')] AS d \
             RETURN sum(DISTINCT d)",
            total,
        ),
        // Numbers are unchanged.
        ("UNWIND [1, 2, null] AS x RETURN sum(x)", Value::Int(3)),
        ("UNWIND [1, 2.5] AS x RETURN sum(x)", Value::Float(3.5)),
        ("UNWIND [1, 2] AS x RETURN avg(x)", Value::Float(1.5)),
        ("UNWIND [] AS x RETURN sum(x)", Value::Int(0)),
        // Integers stay exact past 2^53, and continue as a float past the
        // 64-bit range rather than wrapping (Neo4j's `sum`).
        (
            "UNWIND [9007199254740992, 1] AS x RETURN sum(x)",
            Value::Int(9_007_199_254_740_993),
        ),
        (
            "UNWIND [9223372036854775807, 1] AS x RETURN sum(x)",
            Value::Float(9.223_372_036_854_776e18),
        ),
        ("UNWIND [] AS x RETURN avg(x)", Value::Null),
    ];
    let mut failures = Vec::new();
    for (query, want) in cases {
        match one(&db, query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}: expected {want:?}, got {got:?}")),
            Err(e) => failures.push(format!("{query}: expected {want:?}, got error {e}")),
        }
    }
    for query in [
        "MATCH (t:T) RETURN sum(t.s)",
        "MATCH (t:T) RETURN avg(t.s)",
        "MATCH (u:U) RETURN sum(u.s)",
        "MATCH (v:V) RETURN sum(v.s)",
        "MATCH (v:V) RETURN avg(v.s)",
        "UNWIND ['1', '2'] AS x RETURN sum(x)",
        "UNWIND [true] AS x RETURN avg(x)",
        "UNWIND [1, duration('P1D')] AS x RETURN sum(x)",
    ] {
        if let Ok(got) = one(&db, query).await {
            failures.push(format!("{query}: expected a type error, got {got:?}"));
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}

#[tokio::test]
async fn sum_avg_take_numbers_or_durations_in_locy() -> Result<()> {
    let db = open().await?;
    let run = |query: &'static str| {
        let db = &db;
        async move {
            let result = db
                .session()
                .locy_with(&format!(
                    "CREATE RULE r AS MATCH (t:T) YIELD KEY t, t.d AS d, t.s AS s\n{query}"
                ))
                .with_config(LocyConfig {
                    timeout: Some(Duration::from_secs(60)),
                    ..Default::default()
                })
                .run()
                .await?;
            match result.command_results.last() {
                Some(CommandResult::Query(rows)) if rows.len() == 1 => {
                    Ok(rows[0].get("v").cloned().unwrap_or(Value::Null))
                }
                other => bail!("{query}: expected one row, got {other:?}"),
            }
        }
    };
    let mut failures = Vec::new();
    for (query, want) in [
        ("QUERY r RETURN sum(d) AS v", duration(1, 2 * HOUR_NANOS)),
        ("QUERY r RETURN avg(d) AS v", duration(0, 13 * HOUR_NANOS)),
    ] {
        match run(query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}: expected {want:?}, got {got:?}")),
            Err(e) => failures.push(format!("{query}: expected {want:?}, got error {e}")),
        }
    }
    if let Ok(got) = run("QUERY r RETURN sum(s) AS v").await {
        failures.push(format!("sum(s): expected a type error, got {got:?}"));
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}

/// A window `sum` / `avg` cast its argument for DataFusion's own aggregate:
/// strings summed as integers, and durations or any schemaless property
/// failed with "Unsupported CAST from LargeBinary".
#[tokio::test]
async fn sum_avg_take_numbers_or_durations_in_a_window() -> Result<()> {
    let db = open().await?;
    let total = duration(1, 2 * HOUR_NANOS);
    let mean = duration(0, 13 * HOUR_NANOS);
    let cases: Vec<(&str, Vec<Value>)> = vec![
        (
            "UNWIND [duration('P1D'), duration('PT2H')] AS d RETURN sum(d) OVER () AS v",
            vec![total.clone(); 2],
        ),
        (
            "UNWIND [duration('P1D'), duration('PT2H')] AS d RETURN avg(d) OVER () AS v",
            vec![mean.clone(); 2],
        ),
        ("MATCH (u:U) RETURN avg(u.d) OVER () AS v", vec![mean; 2]),
        (
            "UNWIND [duration('P1D'), duration('P1D'), duration('PT2H')] AS d \
             RETURN sum(DISTINCT d) OVER () AS v",
            vec![total; 3],
        ),
        (
            "UNWIND [3, 20, 100] AS x WITH {a: x} AS m RETURN sum(m.a) OVER (ORDER BY m.a) AS v",
            vec![Value::Int(3), Value::Int(23), Value::Int(123)],
        ),
        (
            "UNWIND [1, 2] AS x RETURN sum(x) OVER () AS v",
            vec![Value::Int(3); 2],
        ),
        (
            "UNWIND [9223372036854775807, 1] AS x RETURN sum(x) OVER () AS v",
            vec![Value::Float(9.223_372_036_854_776e18); 2],
        ),
    ];
    let mut failures = Vec::new();
    for (query, want) in cases {
        match db.session().query(query).await {
            Ok(result) => {
                let got: Vec<Value> = result
                    .rows()
                    .iter()
                    .map(|r| r.values()[0].clone())
                    .collect();
                if got != want {
                    failures.push(format!("{query}: expected {want:?}, got {got:?}"));
                }
            }
            Err(e) => failures.push(format!("{query}: expected {want:?}, got error {e}")),
        }
    }
    for query in [
        "MATCH (v:V) RETURN sum(v.s) OVER () AS v",
        "UNWIND ['1', '2'] AS x RETURN sum(x) OVER () AS v",
    ] {
        if let Ok(result) = db.session().query(query).await {
            failures.push(format!(
                "{query}: expected a type error, got {:?}",
                result.rows()
            ));
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}
