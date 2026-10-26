// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression coverage for silent integer-overflow wrapping in the native
//! DataFusion read-projection arithmetic path.
//!
//! Root cause: `uni-query-functions/src/df_expr.rs::translate_binary_op`
//! (around df_expr.rs:1334-1342) lowers Cypher `+`/`-`/`*` on typed `Int64`
//! columns to DataFusion's native operators (`left + right`, `left - right`,
//! `left * right`), which wrap silently on `i64` overflow instead of raising
//! an error. This is a third arithmetic path, distinct from the interpreted
//! `expr_eval.rs` path and the `apply_int_arithmetic` UDF path, both of which
//! already error on overflow. The correct contract — matching the rest of the
//! engine and Neo4j — is that integer overflow raises an error.
//!
//! `sum` is the exception, in Neo4j as here: an integer sum that overflows
//! continues as a float (`overflowSafeAdd`) rather than raising. A typed
//! `Int64` column's `sum` ran on DataFusion's own, which wraps instead; the
//! `sum` tests below cover every path that reaches it.

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// A read projection that overflows `i64` must error, not silently wrap.
///
/// Builds an in-memory database with a typed `Int64` property so the
/// arithmetic routes through the native DataFusion path (a `CypherValue` /
/// schemaless operand would instead hit the checked UDF path). With
/// `v = i64::MAX`, the query `RETURN n.v + 1` must fail rather than return the
/// wrapped value `i64::MIN`.
///
/// RED today: the native path returns `Ok` with `r == i64::MIN`, so the
/// `is_err()` assertion fails. It passes once df_expr.rs:1334 uses checked
/// arithmetic.
#[tokio::test]
async fn read_int_arithmetic_overflow_errors() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Typed Int64 property `v` so arithmetic routes through the native
    // DataFusion operator path rather than the checked CypherValue UDF path.
    db.schema()
        .label("T")
        .property("v", DataType::Int)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:T {v: $v})")
        .param("v", Value::Int(i64::MAX))
        .run()
        .await?;
    tx.commit().await?;

    // `i64::MAX + 1` wraps to `i64::MIN` under native arithmetic.
    let res = db.session().query("MATCH (n:T) RETURN n.v + 1 AS r").await;
    assert!(
        res.is_err(),
        "integer overflow in a read-path projection must error, not silently \
         wrap (df_expr.rs:1334 native Int64 Add); got {res:?}",
    );

    // Same contract for multiplication: `i64::MAX * 2` overflows.
    let res_mul = db.session().query("MATCH (n:T) RETURN n.v * 2 AS r").await;
    assert!(
        res_mul.is_err(),
        "integer overflow in a read-path projection must error, not silently \
         wrap (df_expr.rs:1342 native Int64 Mul); got {res_mul:?}",
    );

    Ok(())
}

/// 2^53: the first integer above which a float cannot hold every integer.
const TWO_POW_53: i64 = 9_007_199_254_740_992;

/// `i64::MAX + 1` as a float, the value a widened overflowing sum takes.
const MAX_PLUS_ONE: f64 = 9.223_372_036_854_776e18;

/// Groups of the typed `T` and schemaless `S` labels: each group's `v` values
/// and the sum Cypher (Neo4j) gives for them.
fn sum_groups() -> Vec<(i64, Vec<i64>, Value)> {
    vec![
        // Exact above 2^53, where a float sum rounds to 2^53.
        (1, vec![TWO_POW_53, 1], Value::Int(TWO_POW_53 + 1)),
        // Exactly i64::MAX: no overflow, no widening.
        (2, vec![i64::MAX - 1, 1], Value::Int(i64::MAX)),
        // Overflow continues as a float, never wraps to i64::MIN.
        (3, vec![i64::MAX, 1], Value::Float(MAX_PLUS_ONE)),
        // Negative overflow too.
        (4, vec![i64::MIN, -1], Value::Float(-MAX_PLUS_ONE)),
        // Once a float, a float: back in range is still a float.
        (5, vec![i64::MAX, 1, -1], Value::Float(MAX_PLUS_ONE)),
    ]
}

async fn sum_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("T")
        .property("g", DataType::Int)
        .property("v", DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    for (g, values, _) in sum_groups() {
        for v in values {
            for label in ["T", "S"] {
                tx.execute_with(&format!("CREATE (:{label} {{g: $g, v: $v}})"))
                    .param("g", Value::Int(g))
                    .param("v", Value::Int(v))
                    .run()
                    .await?;
            }
        }
    }
    tx.commit().await?;
    Ok(db)
}

/// The first column of every row of `query`.
async fn column(db: &Uni, query: &str) -> Result<Vec<Value>> {
    let result = db.session().query(query).await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| r.values()[0].clone())
        .collect())
}

/// Every way the cases of [`sum_groups`] reach `sum`, against what Cypher
/// answers. Failures are collected, so one run reports every path.
async fn check_sums(db: &Uni, stage: &str, failures: &mut Vec<String>) -> Result<()> {
    let mut expect = |query: String, want: Vec<Value>, got: Result<Vec<Value>>| match got {
        Ok(got) if got == want => {}
        Ok(got) => failures.push(format!("[{stage}] {query}: expected {want:?}, got {got:?}")),
        Err(e) => failures.push(format!(
            "[{stage}] {query}: expected {want:?}, got error {e}"
        )),
    };
    for (g, values, want) in sum_groups() {
        for label in ["T", "S"] {
            let q = format!("MATCH (n:{label}) WHERE n.g = {g} RETURN sum(n.v)");
            let got = column(db, &q).await;
            expect(q, vec![want.clone()], got);
            let q = format!("MATCH (n:{label}) WHERE n.g = {g} RETURN sum(n.v) OVER () AS s");
            let got = column(db, &q).await;
            expect(q, vec![want.clone(); values.len()], got);
        }
        let list = values
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let q = format!("UNWIND [{list}] AS x RETURN sum(x)");
        let got = column(db, &q).await;
        expect(q, vec![want.clone()], got);
    }
    // Grouped: one hash aggregate holding every group at once.
    let want: Vec<Value> = sum_groups().into_iter().map(|(_, _, w)| w).collect();
    for label in ["T", "S"] {
        let q = format!("MATCH (n:{label}) RETURN n.g AS g, sum(n.v) AS s ORDER BY g");
        let got = db.session().query(&q).await.map(|r| {
            r.rows()
                .iter()
                .map(|row| row.values()[1].clone())
                .collect::<Vec<_>>()
        });
        expect(q, want.clone(), got.map_err(Into::into));
        let q = format!(
            "MATCH (n:{label}) RETURN sum(n.v) OVER (PARTITION BY n.g) AS s ORDER BY n.g LIMIT 1"
        );
        let got = column(db, &q).await;
        expect(q, vec![want[0].clone()], got);
    }
    Ok(())
}

/// `sum` over integers stays an exact integer, and on 64-bit overflow
/// continues as a float, as Neo4j's `sum` does (`overflowSafeAdd`). A typed
/// `Int64` column went to DataFusion's own `sum`, which wraps: `i64::MAX + 1`
/// summed to `i64::MIN`.
///
/// Every path is checked with a declared `Int64` column (unflushed and
/// flushed), a schemaless property, an `UNWIND`ed literal list, grouped, and
/// as a window.
#[tokio::test]
async fn sum_of_integers_is_exact_and_widens_on_overflow() -> Result<()> {
    let db = sum_db().await?;
    let mut failures = Vec::new();
    check_sums(&db, "unflushed", &mut failures).await?;
    db.flush().await?;
    check_sums(&db, "flushed", &mut failures).await?;
    if !failures.is_empty() {
        anyhow::bail!("{}", failures.join("\n"));
    }
    Ok(())
}

/// A typed integer `sum` is now a Cypher value, as a schemaless one already
/// was: what consumes it (ordering, comparison, arithmetic, `WITH`, a group
/// with nothing to add) must read it as the number it is.
#[tokio::test]
async fn an_integer_sum_reads_as_a_number_downstream() -> Result<()> {
    let db = sum_db().await?;
    let big = TWO_POW_53 + 1;
    let mut failures = Vec::new();
    for label in ["T", "S"] {
        let cases: Vec<(String, Vec<Value>)> = vec![
            (
                format!(
                    "MATCH (n:{label}) WITH n.g AS g, sum(n.v) AS s \
                     RETURN g ORDER BY s DESC, g"
                ),
                // 3 and 5 tie at i64::MAX + 1.0, above i64::MAX (2), above
                // 2^53 + 1 (1), above the negative overflow (4).
                [3, 5, 2, 1, 4].map(Value::Int).to_vec(),
            ),
            (
                format!(
                    "MATCH (n:{label}) WITH n.g AS g, sum(n.v) AS s \
                     WHERE s = {big} RETURN g"
                ),
                vec![Value::Int(1)],
            ),
            (
                format!(
                    "MATCH (n:{label}) WITH n.g AS g, sum(n.v) AS s \
                     WHERE s > {TWO_POW_53} AND s < {} RETURN g ORDER BY g",
                    i64::MAX
                ),
                vec![Value::Int(1)],
            ),
            (
                format!("MATCH (n:{label}) WHERE n.g = 1 RETURN sum(n.v) + 1"),
                vec![Value::Int(big + 1)],
            ),
            (
                format!("MATCH (n:{label}) WHERE n.g = 1 WITH sum(n.v) AS s RETURN s - {big}"),
                vec![Value::Int(0)],
            ),
            (
                format!("MATCH (n:{label}) WHERE n.g = 3 RETURN sum(n.v) / 2"),
                vec![Value::Float(MAX_PLUS_ONE / 2.0)],
            ),
            (
                format!("MATCH (n:{label}) WHERE n.g = 99 RETURN sum(n.v)"),
                vec![Value::Int(0)],
            ),
            (
                format!("MATCH (n:{label}) WHERE n.g = 1 RETURN sum(DISTINCT n.v)"),
                vec![Value::Int(big)],
            ),
            (
                format!("MATCH (n:{label}) WHERE n.g = 1 RETURN toString(sum(n.v))"),
                vec![Value::String(big.to_string())],
            ),
        ];
        for (query, want) in cases {
            match column(&db, &query).await {
                Ok(got) if got == want => {}
                Ok(got) => failures.push(format!("{query}: expected {want:?}, got {got:?}")),
                Err(e) => failures.push(format!("{query}: expected {want:?}, got error {e}")),
            }
        }
    }
    if !failures.is_empty() {
        anyhow::bail!("{}", failures.join("\n"));
    }
    Ok(())
}
