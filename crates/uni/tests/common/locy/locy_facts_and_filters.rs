// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A Locy rule's facts are rows, its `QUERY` filter is three-valued, and a sum
//! of nothing is 0.
//!
//! * A derived fact is a row (reference §6.2): two derivations of the same row
//!   are one fact. The recursive fixpoint deduplicated through its delta
//!   computation; a non-recursive rule, evaluated in one pass, did not, so three
//!   parallel edges derived `(a, b)` three times.
//! * `QUERY ... WHERE` runs in Locy's in-memory evaluator, where `NULL = x` was
//!   false and `NULL <> x` true, and `AND`/`OR` with a NULL side were NULL even
//!   when the other side decides them. So `QUERY r WHERE a.age <> 0` kept rows
//!   whose `age` is NULL, which the same filter in a rule body (and in Cypher)
//!   rejects.
//! * `SUM` / `MSUM` over a group with no non-null value is 0.0, as Cypher's
//!   `sum` is; it was NULL.
//!
//! Found by the `locy_rule`, `locy_reach` and `locy_fold` relations of
//! `metamorphic::dqp::topo` (W4).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_facts_and_filters)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .property_nullable("age", DataType::Int)
        .edge_type("K", &["P"], &["P"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:P {id: 1, age: 30}), (b:P {id: 2}), \
         (a)-[:K]->(b), (a)-[:K]->(b), (a)-[:K]->(b), (a)-[:K]->(a)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn rows(db: &Uni, program: &str) -> Result<Vec<Vec<Value>>> {
    let result = db.session().locy(program).await?;
    let rows = result
        .command_results()
        .iter()
        .find_map(|c| c.as_query())
        .expect("program has a QUERY");
    let mut out: Vec<Vec<Value>> = rows
        .iter()
        .map(|row| {
            let mut columns: Vec<_> = row.iter().collect();
            columns.sort_by_key(|(name, _)| (*name).clone());
            columns.into_iter().map(|(_, v)| v.clone()).collect()
        })
        .collect();
    out.sort_by_key(|r| format!("{r:?}"));
    Ok(out)
}

#[tokio::test]
async fn locy_facts_and_filters_a_fact_is_a_row() -> Result<()> {
    let db = open().await?;
    let program = "CREATE RULE r AS MATCH (a:P)-[:K]->(b:P) YIELD KEY a, KEY b \
                   QUERY r RETURN a.id AS c0, b.id AS c1";
    assert_eq!(
        rows(&db, program).await?,
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(1), Value::Int(2)],
        ]
    );
    let result = db.session().locy(program).await?;
    assert_eq!(
        result.derived_rule("r").await.unwrap().map(Vec::len),
        Some(2)
    );
    // A FOLD still counts every derivation.
    assert_eq!(
        rows(
            &db,
            "CREATE RULE n AS MATCH (a:P)-[:K]->(b:P) WHERE b.id = 2 FOLD c = COUNT(*) \
             YIELD KEY a, c QUERY n RETURN c AS c0"
        )
        .await?,
        vec![vec![Value::Int(3)]]
    );
    Ok(())
}

#[tokio::test]
async fn locy_facts_and_filters_query_where_is_three_valued() -> Result<()> {
    let db = open().await?;
    let base = "CREATE RULE r AS MATCH (a:P) YIELD KEY a ";
    let one = vec![vec![Value::Int(1)]];
    for (filter, want) in [
        ("a.age <> 0", one.clone()),
        ("NOT (a.age = 99)", one.clone()),
        ("(a.age <> 0) AND (a.id > 0)", one.clone()),
        ("a.age = 30 OR a.age = 31", one.clone()),
        // `false AND NULL` is false and `true OR NULL` is true.
        (
            "NOT (a.id = 1 AND a.age = 0)",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ),
        (
            "a.id = 2 OR a.age = 30",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ),
        ("a.age IS NULL", vec![vec![Value::Int(2)]]),
    ] {
        let program = format!("{base}QUERY r WHERE {filter} RETURN a.id AS c0");
        assert_eq!(rows(&db, &program).await?, want, "{filter}");
    }
    Ok(())
}

#[tokio::test]
async fn locy_facts_and_filters_sum_of_nothing_is_zero() -> Result<()> {
    let db = open().await?;
    for agg in ["SUM", "MSUM"] {
        let program = format!(
            "CREATE RULE s AS MATCH (a:P)-[:K]->(b:P) WHERE b.id = 2 FOLD t = {agg}(b.age) \
             YIELD KEY a, t QUERY s RETURN t AS c0"
        );
        // `age` is an integer column, so the sum is the integer 0, as Cypher's
        // `sum` over no value is (issue #316: an integer sum is an integer).
        assert_eq!(
            rows(&db, &program).await?,
            vec![vec![Value::Int(0)]],
            "{agg}"
        );
    }
    Ok(())
}

/// A `QUERY ... WHERE` that cannot be evaluated is an error, not an empty
/// result. Evaluation errors and non-boolean conditions used to drop every
/// row silently (W6).
#[tokio::test]
async fn locy_facts_and_filters_query_where_errors_are_reported() -> Result<()> {
    let db = open().await?;
    let base = "CREATE RULE r AS MATCH (a:P) YIELD KEY a ";
    for filter in ["a.id / 0 = 1", "a.id", "NOT a.id"] {
        let program = format!("{base}QUERY r WHERE {filter} RETURN a.id AS c0");
        let err = db
            .session()
            .locy(&program)
            .await
            .expect_err(&format!("`{filter}` cannot be evaluated"));
        // Lower-cased: the shared Cypher evaluator Locy now delegates to
        // spells it "Division by zero".
        let text = err.to_string();
        assert!(
            text.to_lowercase().contains("division by zero")
                || text.contains("must be a boolean")
                || text.contains("NOT requires boolean"),
            "{filter}: {text}"
        );
    }
    // Control: NULL is not an error; it drops the row.
    assert_eq!(
        rows(
            &db,
            &format!("{base}QUERY r WHERE a.age > 0 RETURN a.id AS c0")
        )
        .await?,
        vec![vec![Value::Int(1)]]
    );
    Ok(())
}

/// `QUERY ... ORDER BY` an expression that is not a returned column sorts by
/// it. The keys were evaluated on the projected row, where `a.age` no longer
/// existed; the error became NULL, every key tied, and the clause silently did
/// nothing (W6).
#[tokio::test]
async fn locy_facts_and_filters_query_order_by_unreturned_expression() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .property("age", DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {id: 1, age: 20}), (:P {id: 2, age: 40}), (:P {id: 3, age: 30})")
        .await?;
    tx.commit().await?;
    let base = "CREATE RULE r AS MATCH (a:P) YIELD KEY a QUERY r RETURN a.id AS c0 ";
    for (order, want) in [
        ("ORDER BY a.age DESC", [2, 3, 1]),
        ("ORDER BY a.age", [1, 3, 2]),
        ("ORDER BY -a.age", [2, 3, 1]),
        // Control: a returned alias.
        ("ORDER BY c0 DESC", [3, 2, 1]),
    ] {
        let program = format!("{base}{order}");
        let result = db.session().locy(&program).await?;
        let rows = result
            .command_results()
            .iter()
            .find_map(|c| c.as_query())
            .expect("program has a QUERY");
        let got: Vec<Value> = rows.iter().map(|r| r.get("c0").cloned().unwrap()).collect();
        assert_eq!(got, want.map(Value::Int).to_vec(), "{order}");
    }
    // A key that cannot be evaluated is an error.
    let err = db
        .session()
        .locy(&format!("{base}ORDER BY a.id / 0"))
        .await
        .expect_err("ORDER BY a.id / 0");
    assert!(
        err.to_string().to_lowercase().contains("division by zero"),
        "{err}"
    );
    Ok(())
}

/// A `QUERY ... WHERE` keeps the rows the same filter keeps in Cypher.
/// Comparing values of types with no order between them is NULL, not false;
/// it was false, so under `NOT` every such row was kept (W6).
#[tokio::test]
async fn locy_facts_and_filters_query_where_matches_cypher() -> Result<()> {
    let db = open().await?;
    for filter in [
        "a.id < 'x'",
        "NOT (a.id < 'x')",
        "NOT (a.id >= 'x')",
        "NOT (a.id <= true)",
        "NOT (a.id <= {k: 1})",
        "a.id <> {k: 1}",
        "NOT ([a.id, 'x'] > [a.id, 1])",
        // Controls: comparable values.
        "[a.id, 1] < [a.id, 2]",
        "NOT ([a.id] < [a.id, 0])",
        "a.id ^ 2 = 4",
        "a.id >= 2.0",
    ] {
        let cypher = db
            .session()
            .query(&format!("MATCH (a:P) WHERE {filter} RETURN a.id"))
            .await?;
        let mut want: Vec<Vec<Value>> = cypher.rows().iter().map(|r| r.values().to_vec()).collect();
        want.sort_by_key(|r| format!("{r:?}"));
        let program = format!(
            "CREATE RULE r AS MATCH (a:P) YIELD KEY a QUERY r WHERE {filter} RETURN a.id AS c0"
        );
        assert_eq!(rows(&db, &program).await?, want, "{filter}");
    }
    // A power of a non-number is an error, not 0.
    let err = db
        .session()
        .locy(
            "CREATE RULE r AS MATCH (a:P) YIELD KEY a QUERY r WHERE 'x' ^ 2 = 0 RETURN a.id AS c0",
        )
        .await
        .expect_err("'x' ^ 2");
    assert!(err.to_string().contains("pow requires numeric"), "{err}");
    Ok(())
}

/// The `WHERE` of `DERIVE`, `EXPLAIN RULE` and `ABDUCE` reports a condition it
/// cannot evaluate. Each dropped the row instead: `DERIVE` wrote fewer facts,
/// `EXPLAIN` reported no match, and `ABDUCE` a conclusion that no longer held
/// (W6).
#[tokio::test]
async fn locy_facts_and_filters_command_where_errors_are_reported() -> Result<()> {
    let db = open().await?;
    let rule = "CREATE RULE r AS MATCH (a:P)-[:K]->(b:P) DERIVE (a)-[:LINKED]->(b) ";
    let rule_yield = "CREATE RULE r AS MATCH (a:P)-[:K]->(b:P) YIELD KEY a, KEY b ";
    for (program, command) in [
        (rule, "DERIVE r WHERE {f}"),
        (rule_yield, "EXPLAIN RULE r WHERE {f}"),
        (rule_yield, "ABDUCE NOT r WHERE {f}"),
    ] {
        for filter in ["a.id / 0 = 1", "a.id"] {
            let text = format!("{program}{}", command.replace("{f}", filter));
            let err = db
                .session()
                .locy(&text)
                .await
                .expect_err(&format!("`{text}` cannot be evaluated"));
            let msg = err.to_string();
            assert!(
                msg.to_lowercase().contains("division by zero")
                    || msg.contains("must be a boolean"),
                "{text}: {msg}"
            );
        }
        // Control: an evaluable filter runs.
        let text = format!("{program}{}", command.replace("{f}", "a.id = 1"));
        db.session()
            .locy(&text)
            .await
            .map_err(|e| anyhow::anyhow!("{text}: {e}"))?;
    }
    Ok(())
}

/// An ALONG rule's facts are per path (#159), recursive or not. Deduplicating a
/// non-recursive rule's rows (the "a fact is a row" fix above) collapsed two
/// parallel edges carrying the same value into one fact, so a downstream SUM
/// read 2.0 where the same rule with a recursive clause added reads 4.0.
#[tokio::test]
async fn locy_facts_and_filters_along_facts_are_per_path() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Q {id: 1}), (b:Q {id: 2}), (a)-[:E {q: 2}]->(b), (a)-[:E {q: 2}]->(b)")
        .await?;
    tx.commit().await?;
    let base = "CREATE RULE x AS MATCH (p:Q)-[e:E]->(c:Q) ALONG q = e.q YIELD KEY p, KEY c, q ";
    let recursive = "CREATE RULE x AS MATCH (p:Q)-[e:E]->(m:Q) WHERE m IS x TO c \
                     ALONG q = prev.q * e.q YIELD KEY p, KEY c, q ";
    let sum = "CREATE RULE s AS MATCH (p:Q) WHERE p IS x TO c FOLD t = SUM(q) YIELD KEY p, t \
               QUERY s RETURN t AS c0";
    for program in [format!("{base}{sum}"), format!("{base}{recursive}{sum}")] {
        // The integer `q`s sum to an integer (issue #316).
        assert_eq!(
            rows(&db, &program).await?,
            vec![vec![Value::Int(4)]],
            "{program}"
        );
        let result = db.session().locy(&program).await?;
        assert_eq!(
            result.derived_rule("x").await.unwrap().map(Vec::len),
            Some(2),
            "{program}"
        );
    }
    Ok(())
}

/// Scalar expressions evaluate in a Locy `QUERY ... RETURN` as they do in
/// Cypher: the same value, or an error in both (W6 follow-up). Locy raised
/// "division by zero" for a float divisor, where Cypher gives Infinity or NaN,
/// and an error for `toInteger('abc')`, where Cypher gives NULL.
#[tokio::test]
async fn locy_facts_and_filters_scalar_functions_match_cypher() -> Result<()> {
    let db = open().await?;
    for expr in [
        "a.id / 0",
        "a.id % 0",
        "a.id / 0.0",
        "-a.id / 0.0",
        "(a.id - 1) / 0.0",
        "a.id % 0.0",
        "a.id / 2",
        "a.id / 2.0",
        "7 % 3",
        "7.5 % 2",
        "toInteger('abc')",
        "toInteger('12')",
        "toInteger('1.9')",
        "toInteger(2.9)",
        "toFloat('x')",
        "toFloat('2.5')",
        "toFloat(3)",
    ] {
        let cypher = db
            .session()
            .query(&format!("MATCH (a:P) WHERE a.id = 1 RETURN {expr} AS v"))
            .await
            .map(|r| format!("{:?}", r.rows()[0].values()[0]));
        let locy = db
            .session()
            .locy(&format!(
                "CREATE RULE r AS MATCH (a:P) YIELD KEY a QUERY r WHERE a.id = 1 RETURN {expr} AS c0"
            ))
            .await
            .map(|r| {
                let rows = r.command_results().iter().find_map(|c| c.as_query()).cloned();
                format!("{:?}", rows.expect("a QUERY")[0].get("c0").cloned().unwrap())
            });
        match (&cypher, &locy) {
            (Ok(c), Ok(l)) => assert_eq!(l, c, "{expr}"),
            (Err(_), Err(_)) => {}
            _ => panic!("{expr}: cypher {cypher:?}, locy {locy:?}"),
        }
    }
    Ok(())
}

/// An aggregating `QUERY ... RETURN` groups as Cypher's `RETURN` does: the
/// non-aggregate items are the key, and with no key an empty input is one row.
/// Aggregates were evaluated per row, so `count(*)` failed ("unsupported
/// expression: Wildcard") and `sum(x)` returned `x`.
#[tokio::test]
async fn locy_facts_and_filters_query_return_aggregates_match_cypher() -> Result<()> {
    let db = open().await?;
    let body = "MATCH (a:P)-[:K]->(b:P)";
    for (filter, ret) in [
        ("", "count(*) AS c0"),
        ("", "a.id AS c0, count(*) AS c1"),
        (
            "",
            "b.id AS c0, count(*) AS c1, sum(a.id) AS c2, size(collect(a.id)) AS c3",
        ),
        (
            "",
            "count(b.age) AS c0, sum(b.age) AS c1, avg(b.age) AS c2, min(b.id) AS c3, max(b.id) AS c4",
        ),
        ("", "count(DISTINCT a.id) AS c0, sum(DISTINCT a.id) AS c1"),
        ("", "count(*) + 1 AS c0, a.id * 10 AS c1"),
        ("", "sum(a.id + b.id) AS c0, max(b.id) - min(b.id) AS c1"),
        // Empty input: no key is one row; a key is no rows.
        (
            " WHERE a.id = 99",
            "count(*) AS c0, sum(a.id) AS c1, collect(a.id) AS c2, min(a.id) AS c3",
        ),
        (" WHERE a.id = 99", "a.id AS c0, count(*) AS c1"),
    ] {
        let cypher = db
            .session()
            .query(&format!("{body} WITH DISTINCT a, b{filter} RETURN {ret}"))
            .await?;
        let mut want: Vec<Vec<Value>> = cypher.rows().iter().map(|r| r.values().to_vec()).collect();
        want.sort_by_key(|r| format!("{r:?}"));
        let program =
            format!("CREATE RULE r AS {body} YIELD KEY a, KEY b QUERY r{filter} RETURN {ret}");
        let got = rows(&db, &program)
            .await
            .map_err(|e| anyhow::anyhow!("{program}: {e}"))?;
        assert_eq!(got, want, "{program}");
    }
    // ORDER BY an aggregate, and a LIMIT from a parameter.
    let result = db
        .session()
        .locy_with(&format!(
            "CREATE RULE r AS {body} YIELD KEY a, KEY b \
             QUERY r RETURN b.id AS c0, count(*) AS c1 ORDER BY count(*) DESC, c0 LIMIT $n"
        ))
        .param("n", 1)
        .run()
        .await?;
    let rows = result
        .command_results()
        .iter()
        .find_map(|c| c.as_query())
        .expect("a QUERY");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("c0"), Some(&Value::Int(1)));
    // An aggregate in ORDER BY alone is an error, as in Cypher.
    let err = db
        .session()
        .locy(&format!(
            "CREATE RULE r AS {body} YIELD KEY a, KEY b QUERY r RETURN b.id AS c0 ORDER BY count(*)"
        ))
        .await
        .expect_err("ORDER BY count(*) without an aggregate in RETURN");
    assert!(err.to_string().contains("aggregate in ORDER BY"), "{err}");
    Ok(())
}

/// Rule-body conditions joined by `AND` parse when an IS-reference follows an
/// expression. The expression took the `AND` as its own, so
/// `WHERE x AND NOT a IS r TO b` failed to parse; the comma form worked.
#[tokio::test]
async fn locy_facts_and_filters_and_before_a_rule_reference() -> Result<()> {
    let db = open().await?;
    let rules = "CREATE RULE k AS MATCH (a:P)-[:K]->(b:P) YIELD KEY a, KEY b \
                 CREATE RULE self AS MATCH (a:P)-[:K]->(a) YIELD KEY a ";
    for (and_form, comma_form) in [
        (
            "MATCH (a:P), (b:P) WHERE a.id > 0 AND NOT a IS k TO b",
            "MATCH (a:P), (b:P) WHERE a.id > 0, NOT a IS k TO b",
        ),
        (
            "MATCH (a:P), (b:P) WHERE a.id > 0 AND b.id > 0 AND a IS k TO b",
            "MATCH (a:P), (b:P) WHERE a.id > 0, b.id > 0, a IS k TO b",
        ),
        (
            "MATCH (a:P), (b:P) WHERE b.id = 2 AND a IS NOT self AND a.id > 0",
            "MATCH (a:P), (b:P) WHERE b.id = 2, a IS NOT self, a.id > 0",
        ),
        (
            "MATCH (a:P), (b:P) WHERE (a.id = 1 OR a.id = 2) AND a IS self",
            "MATCH (a:P), (b:P) WHERE (a.id = 1 OR a.id = 2), a IS self",
        ),
    ] {
        let run = |body: &str| {
            format!(
                "{rules}CREATE RULE q AS {body} YIELD KEY a, KEY b QUERY q RETURN a.id AS c0, b.id AS c1"
            )
        };
        let want = rows(&db, &run(comma_form)).await?;
        assert!(!want.is_empty(), "{comma_form}");
        assert_eq!(rows(&db, &run(and_form)).await?, want, "{and_form}");
    }
    // An OR before the reference would regroup the condition: still refused.
    let err = db
        .session()
        .locy(&format!(
            "{rules}CREATE RULE q AS MATCH (a:P) WHERE a.id = 1 OR a.id = 2 AND a IS self \
             YIELD KEY a QUERY q RETURN a.id AS c0"
        ))
        .await
        .expect_err("OR before an IS-reference");
    assert!(err.to_string().to_lowercase().contains("pars"), "{err}");
    Ok(())
}

/// Two paths that share their first hop and diverge below it, with equal
/// values, are two facts of an ALONG rule. A derivation was identified by its
/// clause and its own nodes and edges, not by the fact it extended, so
/// `0->1->2` over two parallel `1->2` edges was one fact and a downstream SUM
/// read 4 instead of 7. Found by the W5 random-program oracle.
#[tokio::test]
async fn locy_facts_and_filters_along_paths_diverging_below_the_first_hop() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (n0:N {id: 0}), (n1:N {id: 1}), (n2:N {id: 2}), \
         (n0)-[:E {w: 1}]->(n1), (n1)-[:E {w: 2}]->(n2), (n1)-[:E {w: 2}]->(n2)",
    )
    .await?;
    tx.commit().await?;
    let program = "CREATE RULE cost AS MATCH (a:N)-[e:E]->(b:N) ALONG q = e.w YIELD KEY a, KEY b, q \
                   CREATE RULE cost AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS cost TO b \
                   ALONG q = prev.q + e.w YIELD KEY a, KEY b, q \
                   CREATE RULE tot AS MATCH (a:N) WHERE a IS cost TO b \
                   FOLD n = COUNT(*), s = SUM(q) YIELD KEY a, n, s \
                   QUERY tot WHERE a.id = 0 RETURN n AS c0, s AS c1";
    // The integer path costs sum to an integer (issue #316).
    assert_eq!(
        rows(&db, program).await?,
        vec![vec![Value::Int(3), Value::Int(7)]]
    );
    let result = db.session().locy(program).await?;
    assert_eq!(
        result.derived_rule("cost").await.unwrap().map(Vec::len),
        Some(5)
    );
    Ok(())
}

/// Around a cycle whose values do not grow, an ALONG rule has a new path every
/// lap. Once a derivation is identified by the fact it extended, such a rule
/// has infinitely many facts and reports the iteration limit, where it used to
/// converge on an answer that had collapsed equal paths.
#[tokio::test]
async fn locy_facts_and_filters_along_over_a_zero_weight_cycle_reports_the_limit() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 0})-[:E {w: 0}]->(b:N {id: 1}), (b)-[:E {w: 0}]->(a)")
        .await?;
    tx.commit().await?;
    let err = db
        .session()
        .locy_with(
            "CREATE RULE c AS MATCH (a:N)-[e:E]->(b:N) ALONG q = e.w YIELD KEY a, KEY b, q \
             CREATE RULE c AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS c TO b \
             ALONG q = prev.q + e.w YIELD KEY a, KEY b, q",
        )
        .max_iterations(40)
        .run()
        .await
        .expect_err("infinitely many paths");
    assert!(err.to_string().contains("iteration_limit"), "{err}");
    Ok(())
}
