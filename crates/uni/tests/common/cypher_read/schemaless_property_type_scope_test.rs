// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A property declared on one label does not type the same-named property
//! of another label.
//!
//! The schemaless scan (a label with no schema, a multi-label pattern, or
//! `MATCH (n)`) typed each projected property from the declarations of
//! every label in the schema, merged by name. With `F.v` declared
//! `Float64`, `s.v` on the schemaless `S` was read as a `Float64` column:
//! the stored integer `9007199254740993` came back `9007199254740992.0`,
//! and a string stored there was lost. The write path coerces only under a
//! vertex's own labels, so the stored values were right and only the read
//! was wrong.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(schemaless_property_type_scope)'

// Rust guideline compliant

use uni_db::{DataType, Uni, UniConfig, Value};

const BIG: i64 = (1 << 53) + 1;

fn cfg() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

async fn column(db: &Uni, q: &str) -> Vec<Value> {
    let rows = db
        .session()
        .query(q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    rows.rows().iter().map(|r| r.values()[0].clone()).collect()
}

fn identical(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| match (x, y) {
            (Value::Int(x), Value::Int(y)) => x == y,
            (Value::Float(x), Value::Float(y)) => x.to_bits() == y.to_bits(),
            (x, y) => x == y,
        })
}

#[tokio::test]
async fn schemaless_property_type_scope() {
    let mut failures = Vec::new();
    for flush in [false, true] {
        let db = Uni::in_memory().config(cfg()).build().await.unwrap();
        db.schema()
            .label("F")
            .property("v", DataType::Float64)
            .property("k", DataType::Int64)
            .done()
            .apply()
            .await
            .unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute(&format!(
            "CREATE (:S {{k: 1, v: {BIG}}}), (:S {{k: 2, v: 'x'}}), (:S:T {{k: 3, v: {BIG}}}), \
             (:F {{k: 4, v: 2.5}})"
        ))
        .await
        .unwrap();
        tx.commit().await.unwrap();
        if flush {
            db.flush().await.unwrap();
        }
        for (q, want) in [
            (
                "MATCH (s:S) RETURN s.v ORDER BY s.k",
                vec![Value::Int(BIG), Value::String("x".into()), Value::Int(BIG)],
            ),
            (
                "MATCH (s:S:T) RETURN s.v ORDER BY s.k",
                vec![Value::Int(BIG)],
            ),
            (
                "MATCH (n) RETURN n.v ORDER BY n.k",
                vec![
                    Value::Int(BIG),
                    Value::String("x".into()),
                    Value::Int(BIG),
                    Value::Float(2.5),
                ],
            ),
            ("MATCH (f:F) RETURN f.v", vec![Value::Float(2.5)]),
            (
                &format!("MATCH (s:S) WHERE s.v = {BIG} RETURN s.k ORDER BY s.k"),
                vec![Value::Int(1), Value::Int(3)],
            ),
        ] {
            let got = column(&db, q).await;
            if !identical(&got, &want) {
                failures.push(format!("flush={flush} {q}: want {want:?}, got {got:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Rows of every column, for a query whose expected rows are compared whole.
/// An error is returned as text, so one failing case does not hide the rest.
async fn rows(db: &Uni, q: &str) -> Result<Vec<Vec<Value>>, String> {
    let rows = db.session().query(q).await.map_err(|e| e.to_string())?;
    Ok(rows.rows().iter().map(|r| r.values().to_vec()).collect())
}

/// The `column` of every row a Locy program derives for rule `r`, sorted.
async fn derived(db: &Uni, program: &str, column: &str) -> Result<Vec<Value>, String> {
    let result = db
        .session()
        .locy(program)
        .await
        .map_err(|e| e.to_string())?;
    let mut values: Vec<Value> = result
        .derived_rule("r")
        .await
        .unwrap()
        .ok_or("no facts for r")?
        .iter()
        .map(|row| row.get(column).cloned().unwrap_or(Value::Null))
        .collect();
    // Facts come back in no fixed order.
    values.sort_by_key(|v| format!("{v:?}"));
    Ok(values)
}

fn same_rows(a: &[Vec<Value>], b: &[Vec<Value>]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| identical(x, y))
}

/// `F.v FLOAT` is declared; `S` is schemaless, with `v` an integer above 2^53
/// on one vertex and a string on another. `H` links to both.
async fn node_graph(flush: bool) -> Uni {
    let db = Uni::in_memory().config(cfg()).build().await.unwrap();
    db.schema()
        .label("F")
        .property("v", DataType::Float64)
        .property("k", DataType::Int64)
        .done()
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute(&format!(
        "CREATE (h:H {{k: 0}}), (a:S {{k: 1, v: {BIG}}}), (b:S {{k: 2, v: 'x'}}), \
         (:F {{k: 4, v: 2.5}}), (:F {{k: 5, v: 3.5}}), (h)-[:R]->(a), (h)-[:R]->(b)"
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
    if flush {
        db.flush().await.unwrap();
    }
    db
}

/// A subquery's output columns are typed ahead of running it, from the
/// properties' declarations (`infer_logical_plan_schema`). That lookup took
/// the first label anywhere in the schema declaring the name, so `s.v` on
/// `S` came back from `CALL { ... }` as `F.v`'s `Float64`: the integer
/// rounded, the string NULL, and a filter on the integer matched nothing.
/// A `UNION` of `s.v` and `f.v` was rejected as a planner bug, and `s.v + 0`
/// on the string nulled where Cypher concatenates.
#[tokio::test]
async fn schemaless_property_type_scope_subqueries() {
    let int = Value::Int(BIG);
    let x = || Value::String("x".into());
    let mut failures = Vec::new();
    for flush in [false, true] {
        let db = node_graph(flush).await;
        let cases: Vec<(String, Vec<Vec<Value>>)> = vec![
            (
                "MATCH (h:H) CALL { WITH h MATCH (h)-[:R]->(s:S) RETURN s.v AS v, s.k AS k } \
                 RETURN v ORDER BY k"
                    .into(),
                vec![vec![int.clone()], vec![x()]],
            ),
            (
                "CALL { MATCH (s:S) RETURN s.v AS v, s.k AS k } RETURN v ORDER BY k".into(),
                vec![vec![int.clone()], vec![x()]],
            ),
            (
                "MATCH (h:H) CALL { WITH h MATCH (h)-[:R]->(s) RETURN s.v AS v, s.k AS k } \
                 RETURN v ORDER BY k"
                    .into(),
                vec![vec![int.clone()], vec![x()]],
            ),
            (
                format!(
                    "MATCH (h:H) CALL {{ WITH h MATCH (h)-[:R]->(s:S) RETURN s.v AS v, s.k AS k }} \
                     WITH v, k WHERE v = {BIG} RETURN k"
                ),
                vec![vec![Value::Int(1)]],
            ),
            (
                "MATCH (h:H) CALL { WITH h MATCH (h)-[:R]->(s:S) RETURN DISTINCT s.v AS v } \
                 RETURN v ORDER BY v"
                    .into(),
                vec![vec![x()], vec![int.clone()]],
            ),
            (
                "MATCH (h:H) CALL { WITH h MATCH (h)-[:R]->(s:S) RETURN s.v + 0 AS v, s.k AS k } \
                 RETURN v ORDER BY k"
                    .into(),
                vec![vec![int.clone()], vec![Value::String("x0".into())]],
            ),
            // The declaration still types its own label.
            (
                "CALL { MATCH (f:F) RETURN f.v AS v, f.k AS k } RETURN v ORDER BY k".into(),
                vec![vec![Value::Float(2.5)], vec![Value::Float(3.5)]],
            ),
            (
                "CALL { MATCH (f:F) RETURN avg(f.k) AS a, sum(f.k) AS s } RETURN a, s".into(),
                vec![vec![Value::Float(4.5), Value::Int(9)]],
            ),
            (
                "MATCH (s:S) RETURN s.v AS v UNION ALL MATCH (f:F) WHERE f.k = 4 RETURN f.v AS v"
                    .into(),
                vec![vec![int.clone()], vec![x()], vec![Value::Float(2.5)]],
            ),
            (
                "RETURN 1 AS v UNION ALL RETURN 'a' AS v".into(),
                vec![vec![Value::Int(1)], vec![Value::String("a".into())]],
            ),
        ];
        for (q, want) in cases {
            let got = rows(&db, &q).await;
            if !got.as_ref().is_ok_and(|got| same_rows(got, &want)) {
                failures.push(format!("flush={flush} {q}: want {want:?}, got {got:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The same for relationships. `R.w FLOAT` is declared; `Q` is schemaless.
/// A traversal over every type (`[e]`) typed `w` from `R` alone, so `Q`'s
/// integer rounded and its string read NULL.
#[tokio::test]
async fn schemaless_property_type_scope_edges() {
    let mut failures = Vec::new();
    for flush in [false, true] {
        let db = Uni::in_memory().config(cfg()).build().await.unwrap();
        db.schema()
            .label("N")
            .property("k", DataType::Int64)
            .done()
            .edge_type("R", &["N"], &["N"])
            .property("w", DataType::Float64)
            .done()
            .apply()
            .await
            .unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute(&format!(
            "CREATE (a:N {{k: 1}}), (b:N {{k: 2}}), (c:N {{k: 3}}), \
             (a)-[:R {{w: 2.5, k: 1}}]->(b), (a)-[:Q {{w: {BIG}, k: 2}}]->(c), \
             (b)-[:Q {{w: 'x', k: 3}}]->(c)"
        ))
        .await
        .unwrap();
        tx.commit().await.unwrap();
        if flush {
            db.flush().await.unwrap();
        }
        let all = vec![
            vec![Value::Float(2.5)],
            vec![Value::Int(BIG)],
            vec![Value::String("x".into())],
        ];
        let cases: Vec<(String, Vec<Vec<Value>>)> = vec![
            (
                "MATCH ()-[e]->() RETURN e.w ORDER BY e.k".into(),
                all.clone(),
            ),
            (
                format!("MATCH ()-[e]->() WHERE e.w = {BIG} RETURN e.k"),
                vec![vec![Value::Int(2)]],
            ),
            (
                "MATCH (a:N) CALL { WITH a MATCH (a)-[e]->() RETURN e.w AS w, e.k AS k } \
                 RETURN w ORDER BY k"
                    .into(),
                all,
            ),
            (
                "MATCH ()-[e:R]->() RETURN e.w".into(),
                vec![vec![Value::Float(2.5)]],
            ),
            (
                "MATCH (a:N) WHERE a.k = 1 RETURN a.k AS x \
                 UNION ALL MATCH ()-[e:Q]->() RETURN e.w AS x"
                    .into(),
                vec![
                    vec![Value::Int(1)],
                    vec![Value::Int(BIG)],
                    vec![Value::String("x".into())],
                ],
            ),
        ];
        for (q, want) in cases {
            let got = rows(&db, &q).await;
            if !got.as_ref().is_ok_and(|got| same_rows(got, &want)) {
                failures.push(format!("flush={flush} {q}: want {want:?}, got {got:?}"));
            }
        }
        for (program, want) in [
            (
                "CREATE RULE r AS MATCH (a:N)-[e:Q]->(b:N) YIELD KEY a, KEY b, e.w AS v",
                vec![Value::Int(BIG), Value::String("x".into())],
            ),
            (
                "CREATE RULE r AS MATCH (a:N)-[e:Q]->(b:N) YIELD KEY a, KEY b, e.w + 0 AS v",
                vec![Value::Int(BIG), Value::String("x0".into())],
            ),
            (
                "CREATE RULE r AS MATCH (a:N)-[e:Q]->(b:N) ALONG v = e.w YIELD KEY a, KEY b, v",
                vec![Value::Int(BIG), Value::String("x".into())],
            ),
            (
                "CREATE RULE r AS MATCH (a:N)-[e]->(b:N) YIELD KEY a, KEY b, e.w AS v",
                vec![
                    Value::Float(2.5),
                    Value::Int(BIG),
                    Value::String("x".into()),
                ],
            ),
            (
                "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) ALONG v = e.w YIELD KEY a, KEY b, v",
                vec![Value::Float(2.5)],
            ),
        ] {
            let got = derived(&db, program, "v").await;
            if !got.as_ref().is_ok_and(|got| identical(got, &want)) {
                failures.push(format!(
                    "flush={flush} {program}: want {want:?}, got {got:?}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Locy types a rule's columns from the same declarations. A property of a
/// schemaless label in arithmetic or `ALONG`, or of an unlabelled node, took
/// the type of whichever label declared the name, and the run failed with
/// `cannot read the string "x" as a number`.
#[tokio::test]
async fn schemaless_property_type_scope_locy() {
    let int = Value::Int(BIG);
    let x = Value::String("x".into());
    let mut failures = Vec::new();
    for flush in [false, true] {
        let db = node_graph(flush).await;
        for (program, column, want) in [
            (
                "CREATE RULE r AS MATCH (s:S) YIELD KEY s, s.v AS v",
                "v",
                vec![int.clone(), x.clone()],
            ),
            (
                "CREATE RULE r AS MATCH (s:S) YIELD KEY s, s.v + 0 AS v",
                "v",
                vec![int.clone(), Value::String("x0".into())],
            ),
            (
                "CREATE RULE r AS MATCH (s:S) FOLD v = MAX(s.v) YIELD KEY s, v",
                "v",
                vec![int.clone(), x.clone()],
            ),
            (
                "CREATE RULE r AS MATCH (s) WHERE s.k IN [1, 2] YIELD KEY s, s.v AS v",
                "v",
                vec![int.clone(), x.clone()],
            ),
            (
                "CREATE RULE r AS MATCH (h:H)-[:R]->(s:S) ALONG q = s.v YIELD KEY h, KEY s, q",
                "q",
                vec![int.clone(), x.clone()],
            ),
            (
                "CREATE RULE r AS MATCH (h:H)-[:R]->(s) ALONG q = s.v YIELD KEY h, KEY s, q",
                "q",
                vec![int.clone(), x.clone()],
            ),
            (
                "CREATE RULE r AS MATCH (f:F) YIELD KEY f, f.v AS v",
                "v",
                vec![Value::Float(2.5), Value::Float(3.5)],
            ),
        ] {
            let got = derived(&db, program, column).await;
            if !got.as_ref().is_ok_and(|got| identical(got, &want)) {
                failures.push(format!(
                    "flush={flush} {program}: want {want:?}, got {got:?}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
