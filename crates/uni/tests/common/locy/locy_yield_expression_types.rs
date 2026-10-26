// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A YIELD column carries the value its expression computes, on every
//! surface (issue #314, the yield-typing class).
//!
//! A rule's declared column types are inferred when it is planned, and the
//! facts its body computes must match them. An expression the inference did
//! not recognise was declared a string: a boolean (`x IN [...]`, `EXISTS {}`,
//! `ANY`/`ALL`, `VALID_AT`) was then stored as the text `"true"`, so a
//! consumer's `flag = true` silently matched nothing, and a computed value of
//! any other type (a `COUNT {}`, a parameter, a list element, a function of
//! no argument) disagreed with the declared schema of every rule reading it
//! through `IS`. `labels(n)` and the other functions whose first argument is a
//! node were declared a node id.
//!
//! Each case checks the producer's facts (`derived`), its `QUERY` rows, the
//! column read back through `IS` by a second rule, and that rule's filter on
//! the value.

use anyhow::Result;
use uni_db::{Uni, Value};

/// A YIELD expression and the value it computes for the anchor node
/// (`id: 1`, which has one outgoing `:R`).
struct Case {
    expr: &'static str,
    expected: Value,
    /// A literal the consumer compares the column with, `None` when the value
    /// has no literal form here.
    literal: Option<&'static str>,
}

fn cases() -> Vec<Case> {
    let list = |items: Vec<Value>| Value::List(items);
    vec![
        Case {
            expr: "n.id IN [1, 2]",
            expected: Value::Bool(true),
            literal: Some("true"),
        },
        Case {
            expr: "EXISTS { MATCH (n)-[:R]->() }",
            expected: Value::Bool(true),
            literal: Some("true"),
        },
        Case {
            expr: "ANY(x IN [1, 2] WHERE x > 1)",
            expected: Value::Bool(true),
            literal: Some("true"),
        },
        Case {
            expr: "ALL(x IN [1, 2] WHERE x > 1)",
            expected: Value::Bool(false),
            literal: Some("false"),
        },
        Case {
            expr: "n:M",
            expected: Value::Bool(true),
            literal: Some("true"),
        },
        Case {
            expr: "n VALID_AT datetime('2021-06-01T00:00:00Z')",
            expected: Value::Bool(true),
            literal: Some("true"),
        },
        Case {
            expr: "COUNT { MATCH (n)-[:R]->() }",
            expected: Value::Int(1),
            literal: Some("1"),
        },
        Case {
            expr: "$threshold",
            expected: Value::Int(5),
            literal: Some("5"),
        },
        Case {
            expr: "$ratio",
            expected: Value::Float(0.25),
            literal: Some("0.25"),
        },
        Case {
            expr: "CASE WHEN n.id = 1 THEN 'one' ELSE 'other' END",
            expected: Value::String("one".to_string()),
            literal: Some("'one'"),
        },
        Case {
            expr: "CASE WHEN n.id = 1 THEN 10 ELSE 20 END",
            expected: Value::Int(10),
            literal: Some("10"),
        },
        Case {
            expr: "[10, 20, 30][1]",
            expected: Value::Int(20),
            literal: Some("20"),
        },
        Case {
            expr: "[10, 20, 30][1..]",
            expected: list(vec![Value::Int(20), Value::Int(30)]),
            literal: Some("[20, 30]"),
        },
        Case {
            expr: "labels(n)",
            expected: list(vec![Value::String("M".to_string())]),
            literal: Some("['M']"),
        },
        Case {
            expr: "keys(n)",
            expected: list(vec![
                Value::String("id".to_string()),
                Value::String("valid_from".to_string()),
                Value::String("valid_to".to_string()),
            ]),
            literal: None,
        },
        Case {
            expr: "date('2020-01-02')",
            expected: Value::Temporal(uni_db::common::TemporalValue::Date {
                days_since_epoch: 18263,
            }),
            literal: Some("date('2020-01-02')"),
        },
    ]
}

async fn db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:M {id: 1, valid_from: datetime('2021-01-01T00:00:00Z'), \
         valid_to: datetime('2022-01-01T00:00:00Z')})-[:R]->(:N {id: 2})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// Runs `program` and returns `(derived facts of rule, QUERY rows)` as the
/// value of `column` in each.
async fn run(
    db: &Uni,
    program: &str,
    rule: &str,
    column: &str,
) -> Result<(Vec<Value>, Vec<Value>)> {
    let result = db
        .session()
        .locy_with(program)
        .param("threshold", 5_i64)
        .param("ratio", 0.25_f64)
        .run()
        .await?;
    let pick = |rows: &[uni_locy::FactRow]| {
        rows.iter()
            .map(|r| r.get(column).cloned().unwrap_or(Value::Null))
            .collect::<Vec<_>>()
    };
    let derived = result
        .derived_rule(rule)
        .await?
        .map(|r| pick(r))
        .unwrap_or_default();
    let queried = result.rows().map(|r| pick(r)).unwrap_or_default();
    Ok((derived, queried))
}

/// Every surface of every case, with each disagreement reported.
#[tokio::test]
async fn yield_expression_types_hold_on_every_surface() -> Result<()> {
    let db = db().await?;
    let mut failures = Vec::new();
    for case in cases() {
        let expected = vec![case.expected.clone()];
        let producer = format!(
            "CREATE RULE p AS MATCH (n:M) YIELD KEY n, {} AS v\n",
            case.expr
        );

        match run(&db, &format!("{producer}QUERY p RETURN v"), "p", "v").await {
            Ok((derived, queried)) => {
                if derived != expected {
                    failures.push(format!("{}: derived {derived:?}", case.expr));
                }
                if queried != expected {
                    failures.push(format!("{}: QUERY {queried:?}", case.expr));
                }
            }
            Err(e) => failures.push(format!("{}: producer error {e}", case.expr)),
        }

        let consumer = format!(
            "{producer}CREATE RULE c AS MATCH (n:M) WHERE n IS p YIELD KEY n, v AS w\n\
             QUERY c RETURN w"
        );
        match run(&db, &consumer, "c", "w").await {
            Ok((derived, queried)) => {
                if derived != expected || queried != expected {
                    failures.push(format!(
                        "{}: through IS derived {derived:?} QUERY {queried:?}",
                        case.expr
                    ));
                }
            }
            Err(e) => failures.push(format!("{}: through IS error {e}", case.expr)),
        }

        if let Some(literal) = case.literal {
            let filter = format!(
                "{producer}CREATE RULE f AS MATCH (n:M) WHERE n IS p, v = {literal} \
                 YIELD KEY n\nQUERY f RETURN n.id AS id"
            );
            match run(&db, &filter, "f", "id").await {
                Ok((_, queried)) if queried == vec![Value::Int(1)] => {}
                Ok((_, queried)) => failures.push(format!(
                    "{}: `v = {literal}` through IS matched {queried:?}",
                    case.expr
                )),
                Err(e) => failures.push(format!("{}: filter error {e}", case.expr)),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} disagreement(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

/// `VALID_AT` outside a WHERE predicate: the planner expanded the macro in a
/// predicate only, so a RETURN item using it failed to compile in Cypher as
/// in Locy.
#[tokio::test]
async fn valid_at_is_an_expression_in_a_cypher_projection() -> Result<()> {
    let db = db().await?;
    let rows = db
        .session()
        .query(
            "MATCH (n:M) RETURN n VALID_AT datetime('2021-06-01T00:00:00Z') AS inside, \
             n VALID_AT datetime('2023-06-01T00:00:00Z') AS after",
        )
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.rows()[0].get::<bool>("inside")?, true);
    assert_eq!(rows.rows()[0].get::<bool>("after")?, false);
    Ok(())
}

/// `IS UNIQUE` is constraint syntax, not a value: no expression computes it,
/// in Cypher or in Locy. A YIELD using it is refused with the same error a
/// RETURN gets, on every surface, rather than declaring a column and storing
/// something in it.
#[tokio::test]
async fn is_unique_is_refused_as_a_yield_like_a_return() -> Result<()> {
    const REFUSAL: &str = "IS UNIQUE can only be used in constraint definitions";
    let db = db().await?;
    let cypher = db
        .session()
        .query("MATCH (n:M) RETURN n.id IS UNIQUE AS v")
        .await;
    assert!(
        cypher
            .as_ref()
            .is_err_and(|e| e.to_string().contains(REFUSAL)),
        "Cypher RETURN: {cypher:?}"
    );
    let producer = "CREATE RULE p AS MATCH (n:M) YIELD KEY n, n.id IS UNIQUE AS v\n";
    for program in [
        format!("{producer}QUERY p RETURN v"),
        format!(
            "{producer}CREATE RULE c AS MATCH (n:M) WHERE n IS p YIELD KEY n, v AS w\n\
             QUERY c RETURN w"
        ),
    ] {
        let got = run(&db, &program, "p", "v").await;
        assert!(
            got.as_ref().is_err_and(|e| e.to_string().contains(REFUSAL)),
            "{program}: {got:?}"
        );
    }
    Ok(())
}
