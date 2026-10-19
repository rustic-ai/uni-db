// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Locy compares an integer with a float by exact value, as Cypher does.
//!
//! Widening the integer to `f64` rounds above 2^53, so `2^53 + 1` and
//! `2^53.0` tied, and `i64::MAX` tied with the float `2^63`. Every check here
//! asks the same question of Locy and of plain Cypher over the same graph,
//! and Cypher is the oracle: its numeric comparisons are exact (see
//! `cypher_read::numeric_exact_comparison_test`).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_numeric_exact)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::Uni;
use uni_db::Value;
use uni_db::locy::CommandResult;

type FactRow = std::collections::HashMap<String, Value>;

/// `(name, v)`: two operand pairs that collide when widened to `f64`. In
/// exact order: `b < a < c < d`.
const NUMBERS: &str = "CREATE (g:G {id: 1}), \
     (a:N {name: 'a', v: 9007199254740993}), \
     (b:N {name: 'b', v: 9007199254740992.0}), \
     (c:N {name: 'c', v: 9223372036854775807}), \
     (d:N {name: 'd', v: 9223372036854775808.0}), \
     (g)-[:H]->(a), (g)-[:H]->(b), (g)-[:H]->(c), (g)-[:H]->(d)";

async fn open(create: &str) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(create).await?;
    tx.commit().await?;
    Ok(db)
}

/// The rows of the program's last QUERY, each as its columns in `columns`
/// order.
async fn locy_rows(db: &Uni, program: &str, columns: &[&str]) -> Result<Vec<Vec<Value>>> {
    let result = db.session().locy(program).await?;
    let Some(CommandResult::Query(rows)) = result.command_results().last() else {
        anyhow::bail!("program has no QUERY: {program}");
    };
    Ok(rows.iter().map(|row| project(row, columns)).collect())
}

/// The facts of `rule`, each as its columns in `columns` order, sorted.
async fn locy_facts(
    db: &Uni,
    program: &str,
    rule: &str,
    columns: &[&str],
) -> Result<Vec<Vec<Value>>> {
    let result = db.session().locy(program).await?;
    let mut facts: Vec<Vec<Value>> = result
        .derived
        .get(rule)
        .map(|facts| facts.iter().map(|f| project(f, columns)).collect())
        .unwrap_or_default();
    facts.sort_by_key(|r| format!("{r:?}"));
    Ok(facts)
}

fn project(row: &FactRow, columns: &[&str]) -> Vec<Value> {
    columns
        .iter()
        .map(|c| row.get(*c).cloned().unwrap_or(Value::Null))
        .collect()
}

async fn cypher_rows(db: &Uni, query: &str) -> Result<Vec<Vec<Value>>> {
    let result = db.session().query(query).await?;
    Ok(result.rows().iter().map(|r| r.values().to_vec()).collect())
}

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_key(|r| format!("{r:?}"));
    rows
}

/// A rule over every `N` with its `v`.
const RULE: &str = "CREATE RULE r AS MATCH (n:N) YIELD KEY n, n.v AS v\n";

/// Mismatches between Locy and Cypher, reported together.
#[derive(Default)]
struct Mismatches(Vec<String>);

impl Mismatches {
    fn check(&mut self, what: &str, locy: Vec<Vec<Value>>, cypher: Vec<Vec<Value>>) {
        if locy != cypher {
            self.0.push(format!(
                "{what}\n    locy:   {locy:?}\n    cypher: {cypher:?}"
            ));
        }
    }

    fn assert_none(self) {
        assert!(self.0.is_empty(), "{}", self.0.join("\n"));
    }
}

#[tokio::test]
async fn locy_numeric_exact_query_order_by_matches_cypher() -> Result<()> {
    let db = open(NUMBERS).await?;
    let mut m = Mismatches::default();
    for dir in ["ASC", "DESC"] {
        m.check(
            &format!("ORDER BY v {dir}"),
            locy_rows(
                &db,
                &format!("{RULE}QUERY r RETURN n.name AS name ORDER BY v {dir}"),
                &["name"],
            )
            .await?,
            cypher_rows(
                &db,
                &format!("MATCH (n:N) RETURN n.name ORDER BY n.v {dir}"),
            )
            .await?,
        );
    }
    m.assert_none();
    Ok(())
}

#[tokio::test]
async fn locy_numeric_exact_query_min_max_matches_cypher() -> Result<()> {
    let db = open(NUMBERS).await?;
    let mut m = Mismatches::default();
    // Each pair alone, so the winner cannot be decided by a third value.
    for names in ["['a', 'b']", "['b', 'a']", "['c', 'd']", "['d', 'c']"] {
        m.check(
            &format!("min/max over {names}"),
            locy_rows(
                &db,
                &format!("{RULE}QUERY r WHERE n.name IN {names} RETURN min(v) AS mn, max(v) AS mx"),
                &["mn", "mx"],
            )
            .await?,
            cypher_rows(
                &db,
                &format!("MATCH (n:N) WHERE n.name IN {names} RETURN min(n.v), max(n.v)"),
            )
            .await?,
        );
    }
    m.assert_none();
    Ok(())
}

#[tokio::test]
async fn locy_numeric_exact_rule_where_matches_cypher() -> Result<()> {
    let db = open(NUMBERS).await?;
    let mut m = Mismatches::default();
    for pred in [
        "n.v > 9007199254740992.0",
        "n.v = 9007199254740992",
        "n.v < 9223372036854775808.0",
        "n.v >= 9223372036854775807",
        "n.v = 9223372036854775807",
    ] {
        m.check(
            &format!("rule WHERE {pred}"),
            locy_facts(
                &db,
                &format!("CREATE RULE w AS MATCH (n:N) WHERE {pred} YIELD KEY n, n.name AS name"),
                "w",
                &["name"],
            )
            .await?,
            sorted(cypher_rows(&db, &format!("MATCH (n:N) WHERE {pred} RETURN n.name")).await?),
        );
        m.check(
            &format!("QUERY WHERE {pred}"),
            sorted(
                locy_rows(
                    &db,
                    &format!("{RULE}QUERY r WHERE {pred} RETURN n.name AS name"),
                    &["name"],
                )
                .await?,
            ),
            sorted(cypher_rows(&db, &format!("MATCH (n:N) WHERE {pred} RETURN n.name")).await?),
        );
    }
    m.assert_none();
    Ok(())
}

#[tokio::test]
async fn locy_numeric_exact_fold_and_best_by_match_cypher() -> Result<()> {
    let db = open(NUMBERS).await?;
    let mut m = Mismatches::default();
    for names in ["['a', 'b']", "['b', 'a']", "['c', 'd']", "['d', 'c']"] {
        m.check(
            &format!("FOLD MIN/MAX over {names}"),
            locy_facts(
                &db,
                &format!(
                    "CREATE RULE f AS MATCH (g:G)-[:H]->(n:N) WHERE n.name IN {names} \
                     FOLD mn = MIN(n.v), mx = MAX(n.v) YIELD KEY g, mn, mx"
                ),
                "f",
                &["mn", "mx"],
            )
            .await?,
            cypher_rows(
                &db,
                &format!(
                    "MATCH (g:G)-[:H]->(n:N) WHERE n.name IN {names} RETURN min(n.v), max(n.v)"
                ),
            )
            .await?,
        );
        for (dir, agg) in [("ASC", "min"), ("DESC", "max")] {
            m.check(
                &format!("BEST BY v {dir} over {names}"),
                locy_facts(
                    &db,
                    &format!(
                        "CREATE RULE top AS MATCH (g:G)-[:H]->(n:N) WHERE n.name IN {names} \
                         BEST BY v {dir} YIELD KEY g, n.v AS v, n.name AS name"
                    ),
                    "top",
                    &["name"],
                )
                .await?,
                cypher_rows(
                    &db,
                    &format!(
                        "MATCH (g:G)-[:H]->(n:N) WHERE n.name IN {names} \
                         WITH {agg}(n.v) AS want \
                         MATCH (:G)-[:H]->(n:N) WHERE n.v = want RETURN n.name"
                    ),
                )
                .await?,
            );
        }
    }
    m.assert_none();
    Ok(())
}

/// `1` and `1.0` are one value to DISTINCT, as in Cypher.
#[tokio::test]
async fn locy_numeric_exact_distinct_matches_cypher() -> Result<()> {
    let db = open(
        "CREATE (:N {name: 'p', v: 1}), (:N {name: 'q', v: 1.0}), \
         (:N {name: 'r', v: 9007199254740993}), (:N {name: 's', v: 9007199254740992.0})",
    )
    .await?;
    let mut m = Mismatches::default();
    m.check(
        "RETURN DISTINCT count",
        vec![vec![Value::Int(
            locy_rows(&db, &format!("{RULE}QUERY r RETURN DISTINCT v"), &["v"])
                .await?
                .len() as i64,
        )]],
        vec![vec![Value::Int(
            cypher_rows(&db, "MATCH (n:N) RETURN DISTINCT n.v")
                .await?
                .len() as i64,
        )]],
    );
    for agg in ["count(DISTINCT v)", "size(collect(DISTINCT v))"] {
        let cypher_agg = agg.replace(" v)", " n.v)");
        m.check(
            agg,
            locy_rows(&db, &format!("{RULE}QUERY r RETURN {agg} AS x"), &["x"]).await?,
            cypher_rows(&db, &format!("MATCH (n:N) RETURN {cypher_agg}")).await?,
        );
    }
    m.assert_none();
    Ok(())
}

/// A scalar IS-reference test joins an integer key with a float key by
/// exact value.
#[tokio::test]
async fn locy_numeric_exact_is_reference_scalar_join_matches_cypher() -> Result<()> {
    let db = open(
        "CREATE (:N {v: 9007199254740993}), (:N {v: 1}), \
         (:M {u: 9007199254740992.0}), (:M {u: 1.0})",
    )
    .await?;
    let program = "CREATE RULE ni AS MATCH (n:N) YIELD KEY n.v AS v\n\
                   CREATE RULE mf AS MATCH (m:M) YIELD KEY m.u AS v\n\
                   CREATE RULE both AS MATCH (x:N) WHERE v IS ni, v IS mf YIELD KEY v";
    let mut m = Mismatches::default();
    m.check(
        "v IS ni, v IS mf",
        locy_facts(&db, program, "both", &["v"]).await?,
        sorted(
            cypher_rows(
                &db,
                "MATCH (n:N), (m:M) WHERE n.v = m.u RETURN DISTINCT n.v",
            )
            .await?,
        ),
    );
    m.assert_none();
    Ok(())
}

/// BEST BY over a column of mixed numbers picks by value. The column holds
/// encoded values, which sorted by their bytes: every integer ranked below
/// every float, and negative numbers above positive ones.
#[tokio::test]
async fn locy_numeric_exact_best_by_orders_by_value() -> Result<()> {
    for values in [
        "[5, 1.5, 256, 2.5]",
        "[-1, 1, 256, 2]",
        "[0.5, -2.5, 3.0, -1.0]",
    ] {
        let db = open(&format!(
            "CREATE (g:G) WITH g UNWIND range(0, size({values}) - 1) AS i \
             CREATE (g)-[:H]->(:N {{name: toString(i), v: {values}[i]}})"
        ))
        .await?;
        let mut m = Mismatches::default();
        for (dir, agg) in [("ASC", "min"), ("DESC", "max")] {
            m.check(
                &format!("BEST BY v {dir} over {values}"),
                locy_facts(
                    &db,
                    &format!(
                        "CREATE RULE top AS MATCH (g:G)-[:H]->(n:N) \
                         BEST BY v {dir} YIELD KEY g, n.v AS v, n.name AS name"
                    ),
                    "top",
                    &["name"],
                )
                .await?,
                cypher_rows(
                    &db,
                    &format!(
                        "MATCH (:G)-[:H]->(n:N) WITH {agg}(n.v) AS want \
                         MATCH (:G)-[:H]->(n:N) WHERE n.v = want RETURN n.name"
                    ),
                )
                .await?,
            );
        }
        m.check(
            &format!("FOLD MIN/MAX over {values}"),
            locy_facts(
                &db,
                "CREATE RULE f AS MATCH (g:G)-[:H]->(n:N) \
                 FOLD mn = MIN(n.v), mx = MAX(n.v) YIELD KEY g, mn, mx",
                "f",
                &["mn", "mx"],
            )
            .await?,
            cypher_rows(&db, "MATCH (g:G)-[:H]->(n:N) RETURN min(n.v), max(n.v)").await?,
        );
        m.assert_none();
    }
    Ok(())
}

/// A recursive BEST BY rule picks by value too, among the candidates one
/// iteration derives for a KEY.
#[tokio::test]
async fn locy_numeric_exact_recursive_best_by_orders_by_value() -> Result<()> {
    let db = open(
        "CREATE (a:P {name: 'a'}), (b:P {name: 'b'}), (c:P {name: 'c'}), \
         (a)-[:E {w: 5}]->(b), (a)-[:E {w: 1.5}]->(b), (a)-[:E {w: -1}]->(b), \
         (b)-[:E {w: 2}]->(c), (b)-[:E {w: 256}]->(c)",
    )
    .await?;
    for (dir, want) in [("ASC", [-1.0, -1.0, 2.0]), ("DESC", [5.0, 5.0, 256.0])] {
        let program = format!(
            "CREATE RULE pick AS MATCH (a:P)-[e:E]->(b:P) BEST BY w {dir} \
             YIELD KEY a, KEY b, e.w AS w\n\
             CREATE RULE pick AS MATCH (a:P)-[e:E]->(m:P) WHERE m IS pick TO b \
             BEST BY w {dir} YIELD KEY a, KEY b, e.w AS w\n\
             QUERY pick RETURN a.name AS a, b.name AS b, w ORDER BY a, b"
        );
        let got: Vec<f64> = locy_rows(&db, &program, &["w"])
            .await?
            .into_iter()
            .map(|r| r[0].as_f64().expect("a number"))
            .collect();
        assert_eq!(got, want, "BEST BY w {dir}");
    }
    Ok(())
}

/// A recursive BEST BY rule compares a new candidate with the fact it holds
/// for the KEY by value: `(a, c)` holds `3` from the first iteration and the
/// second derives `-1`. The merge compared the encoded bytes, under which
/// `3` ranks before `-1`.
#[tokio::test]
async fn locy_numeric_exact_recursive_best_by_displaces_by_value() -> Result<()> {
    let db = open(
        "CREATE (a:P {name: 'a'}), (b:P {name: 'b'}), (c:P {name: 'c'}), \
         (a)-[:E {w: 5}]->(b), (a)-[:E {w: 1.5}]->(b), (a)-[:E {w: -1}]->(b), \
         (b)-[:E {w: 2}]->(c), (b)-[:E {w: 256}]->(c), (a)-[:E {w: 3}]->(c)",
    )
    .await?;
    for (dir, want) in [("ASC", [-1.0, -1.0, 2.0]), ("DESC", [5.0, 5.0, 256.0])] {
        let program = format!(
            "CREATE RULE pick AS MATCH (a:P)-[e:E]->(b:P) BEST BY w {dir} \
             YIELD KEY a, KEY b, e.w AS w\n\
             CREATE RULE pick AS MATCH (a:P)-[e:E]->(m:P) WHERE m IS pick TO b \
             BEST BY w {dir} YIELD KEY a, KEY b, e.w AS w\n\
             QUERY pick RETURN a.name AS a, b.name AS b, w ORDER BY a, b"
        );
        let got: Vec<f64> = locy_rows(&db, &program, &["w"])
            .await?
            .into_iter()
            .map(|r| r[0].as_f64().expect("a number"))
            .collect();
        assert_eq!(got, want, "BEST BY w {dir}");
    }
    Ok(())
}
