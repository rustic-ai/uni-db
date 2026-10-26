// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A numeric FOLD refuses a value that is not a number, and `SUM` / `AVG`
//! add durations.
//!
//! `SUM`, `AVG`, `MSUM` and `MNOR` read their input as a number. A Cypher
//! value (a schemaless property) or a duration column was brought to Float64
//! through `_cypher_to_float64`, which turns a non-number into NULL, so a FOLD
//! over strings or durations summed to 0.0, and `AVG` and `MNOR` answered
//! NULL, recursive or not. A Cypher value now reaches `SUM` / `AVG` as it is
//! and folds as Cypher's `sum` / `avg` do (one `CypherSum`): durations add up,
//! strings are an error. `MNOR` reads through `_cypher_number`, which keeps
//! NULL and fails on anything but a number; `MSUM` reads a Cypher value as
//! it is and takes numbers alone.
//!
//! `SUM` and `MSUM` over integers fold to an exact integer, as Cypher's `sum`
//! does, continuing as a float only past the 64-bit range (issue #316).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_fold_reads_numbers)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Result, bail};
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{DataType, Uni, Value};

/// `a` reaches `b` and `c`, and `b` reaches `c`: `a` folds two edges.
async fn graph(w_type: Option<DataType>, w1: &str, w2: &str) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if let Some(dt) = w_type {
        db.schema()
            .label("N")
            .property("name", DataType::String)
            .done()
            .edge_type("E", &["N"], &["N"])
            .property_nullable("w", dt)
            .done()
            .apply()
            .await?;
    }
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE (a:N {{name: 'a'}}), (b:N {{name: 'b'}}), (c:N {{name: 'c'}}), \
         (a)-[:E {{w: {w1}}}]->(b), (a)-[:E {{w: {w2}}}]->(c), (b)-[:E {{w: {w2}}}]->(c)"
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// A FOLD of `e.w` per source node, recursive for a monotonic aggregate.
fn program(aggregate: &str) -> String {
    let base = format!(
        "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD t = {aggregate}(e.w) YIELD KEY a, t\n"
    );
    let recursive = if aggregate.starts_with('M') {
        format!(
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) WHERE b IS r \
             FOLD t = {aggregate}(e.w) YIELD KEY a, t\n"
        )
    } else {
        String::new()
    };
    format!("{base}{recursive}QUERY r WHERE a.name = 'a' RETURN t")
}

/// The FOLD's value for `a`.
async fn fold(db: &Uni, aggregate: &str) -> Result<Value> {
    let result = db
        .session()
        .locy_with(&program(aggregate))
        .with_config(LocyConfig {
            max_iterations: 1000,
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .run()
        .await?;
    match result.command_results.last() {
        Some(CommandResult::Query(rows)) if rows.len() == 1 => {
            Ok(rows[0].get("t").cloned().unwrap_or(Value::Null))
        }
        other => bail!("{aggregate}: expected one row, got {other:?}"),
    }
}

/// Cypher's `sum` / `avg` over `a`'s edges: the oracle for FOLD `SUM` / `AVG`.
async fn cypher(db: &Uni, aggregate: &str) -> Result<Value> {
    let result = db
        .session()
        .query(&format!(
            "MATCH (a:N {{name: 'a'}})-[e:E]->(:N) RETURN {aggregate}(e.w)"
        ))
        .await?;
    let [row] = result.rows() else {
        bail!("{aggregate}: expected one row");
    };
    Ok(row.values()[0].clone())
}

#[tokio::test]
async fn locy_fold_reads_numbers_or_fails() -> Result<()> {
    let mut failures = Vec::new();
    let mut refuse = |label: &str, aggregate: &str, got: Result<Value>| match got {
        Ok(got) => failures.push(format!(
            "{aggregate} over {label}: expected an error, got {got:?}"
        )),
        Err(e) if e.to_string().contains("number") => {}
        Err(e) => failures.push(format!(
            "{aggregate} over {label}: expected a not-a-number error, got {e}"
        )),
    };
    let strings = [
        ("schemaless strings", None),
        ("typed strings", Some(DataType::String)),
    ];
    for (label, w_type) in strings {
        let db = graph(w_type, "'5'", "'7'").await?;
        for aggregate in ["SUM", "AVG", "MSUM", "MNOR"] {
            refuse(label, aggregate, fold(&db, aggregate).await);
        }
    }
    let durations = [
        ("schemaless durations", None),
        ("typed durations", Some(DataType::Duration)),
    ];
    let mut answers = Vec::new();
    for (label, w_type) in durations {
        let db = graph(w_type, "duration('P1D')", "duration('PT12H')").await?;
        // A recursive sum, and a probability, take numbers only.
        for aggregate in ["MSUM", "MNOR"] {
            refuse(label, aggregate, fold(&db, aggregate).await);
        }
        // `SUM` / `AVG` add durations, as Cypher's `sum` / `avg` do.
        for aggregate in ["SUM", "AVG"] {
            answers.push((
                format!("{aggregate} over {label}"),
                cypher(&db, aggregate).await?,
                fold(&db, aggregate).await,
            ));
        }
    }
    // `AVG` of numbers is a float. `SUM` of integers, schemaless or not, is
    // an integer, as Cypher's `sum` is: it used to be the float `12.0`,
    // which rounds a total above 2^53 (issue #316, and
    // `locy_fold_sums_integers_exactly`). `SUM` of floats is a float.
    for (label, w_type) in [
        ("schemaless ints", None),
        ("typed int32", Some(DataType::Int32)),
        ("typed floats", Some(DataType::Float64)),
    ] {
        let db = graph(w_type.clone(), "5", "7").await?;
        let sum = if matches!(w_type, Some(DataType::Float64)) {
            Value::Float(12.0)
        } else {
            Value::Int(12)
        };
        for (aggregate, want) in [("SUM", sum), ("AVG", Value::Float(6.0))] {
            answers.push((
                format!("{aggregate} over {label}"),
                want,
                fold(&db, aggregate).await,
            ));
        }
    }
    for (label, want, got) in answers {
        match got {
            Ok(got) if got == want => {}
            other => failures.push(format!("{label}: expected {want:?}, got {other:?}")),
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}

/// 2^53: the first integer above which a float cannot hold every integer.
const TWO_POW_53: i64 = 9_007_199_254_740_992;

/// `i64::MAX + 1` as a float, the value an overflowing sum continues as.
const MAX_PLUS_ONE: f64 = 9.223_372_036_854_776e18;

/// Runs `program` and returns the last QUERY's `t` per row.
async fn query_t(db: &Uni, program: &str) -> Result<Vec<Value>> {
    let result = db
        .session()
        .locy_with(program)
        .with_config(LocyConfig {
            max_iterations: 1000,
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .run()
        .await?;
    match result.command_results.last() {
        Some(CommandResult::Query(rows)) => Ok(rows
            .iter()
            .map(|r| r.get("t").cloned().unwrap_or(Value::Null))
            .collect()),
        other => bail!("expected a QUERY result, got {other:?}"),
    }
}

/// Records a failure unless `got` is `want`, value and type alike.
fn check(failures: &mut Vec<String>, what: &str, want: &[Value], got: Result<Vec<Value>>) {
    match got {
        Ok(got) if got == want => {}
        other => failures.push(format!("{what}: expected {want:?}, got {other:?}")),
    }
}

/// FOLD `SUM` / `MSUM` over integers are exact integers (issue #316): two
/// ledger amounts of 2^53 and 1 folded to the float 2^53, a cent lost. They
/// now fold as Cypher's `sum` does — an integer, continuing as a float only
/// past the 64-bit range — and agree with `QUERY ... RETURN sum(...)`.
#[tokio::test]
async fn locy_fold_sums_integers_exactly() -> Result<()> {
    let mut failures = Vec::new();
    for (label, w_type) in [
        ("schemaless ints", None),
        ("typed int64", Some(DataType::Int64)),
        ("typed int32", Some(DataType::Int32)),
    ] {
        // An Int32 cannot hold 2^53, so there it is the type that is checked.
        let int32 = matches!(w_type, Some(DataType::Int32));
        let (w1, want) = if int32 {
            ("5".to_string(), 12)
        } else {
            (TWO_POW_53.to_string(), TWO_POW_53 + 7)
        };
        let db = graph(w_type.clone(), &w1, "7").await?;
        let want = [Value::Int(want)];
        check(
            &mut failures,
            &format!("SUM over {label}"),
            &want,
            fold(&db, "SUM").await.map(|v| vec![v]),
        );
        check(
            &mut failures,
            &format!("Cypher sum over {label}"),
            &want,
            cypher(&db, "sum").await.map(|v| vec![v]),
        );
        // QUERY's own `sum` over the same rows agrees with the FOLD.
        check(
            &mut failures,
            &format!("QUERY sum over {label}"),
            &want,
            query_t(
                &db,
                "CREATE RULE w AS MATCH (a:N)-[e:E]->(b:N) \
                 YIELD KEY e, a.name AS name, e.w AS w\n\
                 QUERY w WHERE name = 'a' RETURN sum(w) AS t",
            )
            .await,
        );
        // Every source keeps its own exact total.
        check(
            &mut failures,
            &format!("SUM per source over {label}"),
            &[want[0].clone(), Value::Int(7)],
            query_t(
                &db,
                "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD t = SUM(e.w) YIELD KEY a, t\n\
                 QUERY r RETURN a.name AS name, t ORDER BY name",
            )
            .await,
        );
        // The folded value reads as the number it is: compared, ordered, and
        // carried through an IS reference.
        let Value::Int(n) = want[0] else {
            unreachable!()
        };
        check(
            &mut failures,
            &format!("WHERE on a folded SUM over {label}"),
            &want,
            query_t(
                &db,
                &format!(
                    "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD t = SUM(e.w) YIELD KEY a, t\n\
                     QUERY r WHERE t = {n} RETURN t"
                ),
            )
            .await,
        );
        check(
            &mut failures,
            &format!("ORDER BY a folded SUM over {label}"),
            &[want[0].clone(), Value::Int(7)],
            query_t(
                &db,
                "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD t = SUM(e.w) YIELD KEY a, t\n\
                 QUERY r RETURN t ORDER BY t DESC",
            )
            .await,
        );
        check(
            &mut failures,
            &format!("IS reference to a folded SUM over {label}"),
            &want,
            query_t(
                &db,
                "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD t = SUM(e.w) YIELD KEY a, t\n\
                 CREATE RULE big AS MATCH (a:N) WHERE a IS r, t > 7 YIELD KEY a, t\n\
                 QUERY big RETURN t",
            )
            .await,
        );
        // A FOLD over a value column another rule yields, and an integer
        // expression: typed by the referenced rule and by the arithmetic,
        // not by a property.
        check(
            &mut failures,
            &format!("SUM over a referenced value column over {label}"),
            &want,
            query_t(
                &db,
                "CREATE RULE w AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY b, e.w AS w\n\
                 CREATE RULE s AS MATCH (a:N) WHERE a IS w TO b FOLD t = SUM(w) YIELD KEY a, t\n\
                 QUERY s WHERE a.name = 'a' RETURN t",
            )
            .await,
        );
        check(
            &mut failures,
            &format!("SUM over an integer expression over {label}"),
            &[Value::Int(n - 2)],
            query_t(
                &db,
                "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD t = SUM(e.w - 1) YIELD KEY a, t\n\
                 QUERY r WHERE a.name = 'a' RETURN t",
            )
            .await,
        );
        // An expression over the folded integer, after the FOLD. Typed a
        // Cypher value, `t + 1` failed to plan ("Cannot coerce arithmetic
        // expression LargeBinary + Int64").
        check(
            &mut failures,
            &format!("arithmetic on a folded SUM over {label}"),
            &[Value::Int(n + 1)],
            query_t(
                &db,
                "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD s = SUM(e.w) \
                 YIELD KEY a, s + 1 AS t\n\
                 QUERY r WHERE a.name = 'a' RETURN t",
            )
            .await,
        );
        // A recursive MSUM totals k1 * w1 + k2 * 7 for the number of times the
        // fixpoint folds each edge. Learn k1 and k2 with w1 = 1000, then
        // require the exact integer for w1 = 2^53.
        if !int32 {
            let small = graph(w_type.clone(), "1000", "7").await?;
            let got = fold(&db, "MSUM").await.map(|v| vec![v]);
            match fold(&small, "MSUM").await {
                Ok(Value::Int(r)) => {
                    let (k1, k2) = (r / 1000, r % 1000 / 7);
                    check(
                        &mut failures,
                        &format!("recursive MSUM over {label}"),
                        &[Value::Int(k1 * TWO_POW_53 + k2 * 7)],
                        got,
                    );
                }
                other => failures.push(format!(
                    "recursive MSUM over {label} (w = 1000, 7): expected an integer, got {other:?}"
                )),
            }
        }
    }
    // Past the 64-bit range: a float, as Cypher's `sum`, never a wrap.
    for (label, w_type) in [
        ("schemaless ints", None),
        ("typed int64", Some(DataType::Int64)),
    ] {
        let db = graph(w_type, &i64::MAX.to_string(), "1").await?;
        check(
            &mut failures,
            &format!("SUM overflowing over {label}"),
            &[Value::Float(MAX_PLUS_ONE)],
            fold(&db, "SUM").await.map(|v| vec![v]),
        );
        // The recursive total adds i64::MAX at least once more: a float at or
        // above i64::MAX + 1, never a wrapped negative integer.
        match fold(&db, "MSUM").await {
            Ok(Value::Float(f)) if f >= MAX_PLUS_ONE => {}
            other => failures.push(format!(
                "MSUM overflowing over {label}: expected a float past i64::MAX, got {other:?}"
            )),
        }
    }
    // A float among the integers: a float.
    let db = graph(None, "2.5", "7").await?;
    check(
        &mut failures,
        "SUM over mixed ints and floats",
        &[Value::Float(9.5)],
        fold(&db, "SUM").await.map(|v| vec![v]),
    );
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}

/// A rule aggregates through FOLD alone. An aggregate in a YIELD item or an
/// ALONG binding reached the physical planner and failed there ("Physical
/// plan does not support logical expression AggregateFunction"); the planner
/// had also typed such a `sum` Float64. It is now a compile error naming the
/// position, as an aggregate in the rule-body WHERE already was.
#[tokio::test]
async fn locy_aggregate_outside_fold_is_a_compile_error() -> Result<()> {
    let db = graph(Some(DataType::Int64), "5", "7").await?;
    let mut failures = Vec::new();
    for (program, position) in [
        (
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, sum(e.w) AS t\n\
             QUERY r RETURN t",
            "YIELD column 't'",
        ),
        (
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, abs(count(e)) AS t\n\
             QUERY r RETURN t",
            "YIELD column 't'",
        ),
        (
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) ALONG c = sum(e.w) \
             YIELD KEY a, KEY b, c AS t\nQUERY r RETURN t",
            "ALONG binding 'c'",
        ),
    ] {
        match query_t(&db, program).await {
            Err(e)
                if e.to_string().contains("InvalidAggregation")
                    && e.to_string().contains(position) => {}
            other => failures.push(format!(
                "{program}: expected an InvalidAggregation error naming {position}, got {other:?}"
            )),
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}
