// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `[NOT] EXISTS { MATCH (o:L) WHERE … }` tied to the outer row by its
//! `WHERE` alone is a semi- or anti-join, not a subquery per row.
//!
//! ```cypher
//! MATCH (m:M)
//! WHERE NOT EXISTS { MATCH (o:M) WHERE o.t = m.t AND o.k < m.k }
//! ```
//!
//! The subquery ran once per `m`, scanning every `M` each time: "no other
//! row like this one" over N rows was N scans of N rows. In the
//! entity-resolution program that was most of the new-entity `MERGE`
//! (`docs/proposals/locy_resolution_cost_2026_11_14.md` §3.3).
//!
//! The oracle is the same query with `COUNT { … } > 0` or `= 0` in place of
//! the `EXISTS`: the same subquery, which the join rewrite does not take and
//! which still runs per row. A few cases are also checked against the answer
//! computed here, so the two paths cannot be wrong together unnoticed.

use std::collections::BTreeSet;

use anyhow::{Result, anyhow, bail};
use uni_db::{DataType, Session, Uni, UniConfig, Value};
use uni_locy::CommandResult;

const N: i64 = 12;
/// `k` of the nodes with an `R` edge to `k + 1`.
const LINKED: &[i64] = &[0, 5];

/// `t` of node `k`: one of three types, absent for every fifth node.
fn t(k: i64) -> Option<&'static str> {
    (k % 5 != 4).then(|| ["a", "b", "c"][(k % 3) as usize])
}

/// `s` of node `k`: one of four names, absent for every seventh node.
fn s(k: i64) -> Option<String> {
    (k % 7 != 6).then(|| format!("n{}", k % 4))
}

async fn open(flush: bool) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_interval: None,
            auto_flush_threshold: usize::MAX,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("M")
        .property("k", DataType::Int64)
        .property_nullable("t", DataType::String)
        .property_nullable("s", DataType::String)
        .done()
        .edge_type("R", &["M"], &["M"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    for k in 0..N {
        let mut props = vec![format!("k: {k}")];
        if let Some(t) = t(k) {
            props.push(format!("t: '{t}'"));
        }
        if let Some(s) = s(k) {
            props.push(format!("s: '{s}'"));
        }
        tx.execute(&format!("CREATE (:M {{{}}})", props.join(", ")))
            .await?;
    }
    for k in LINKED {
        tx.execute(&format!(
            "MATCH (a:M {{k: {k}}}), (b:M {{k: {}}}) CREATE (a)-[:R]->(b)",
            k + 1
        ))
        .await?;
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

type Keys = BTreeSet<i64>;

/// `template` with `$NX` and `$X` replaced by the negated and plain subquery
/// over `body`, as `EXISTS` or as the `COUNT` comparison that means the same.
fn predicate(template: &str, body: &str, as_count: bool) -> String {
    let (plain, negated) = if as_count {
        (
            format!("COUNT {{ {body} }} > 0"),
            format!("COUNT {{ {body} }} = 0"),
        )
    } else {
        (
            format!("EXISTS {{ {body} }}"),
            format!("NOT EXISTS {{ {body} }}"),
        )
    };
    template.replace("$NX", &negated).replace("$X", &plain)
}

fn keys(rows: impl Iterator<Item = Option<Value>>, what: &str) -> Result<Keys> {
    rows.map(|value| match value {
        Some(Value::Int(k)) => Ok(k),
        other => Err(anyhow!("{what}: unexpected value {other:?}")),
    })
    .collect()
}

async fn cypher(session: &Session, predicate: &str) -> Result<Keys> {
    let query = format!("MATCH (m:M) WHERE {predicate} RETURN m.k");
    let result = session
        .query(&query)
        .await
        .map_err(|e| anyhow!("{query}: {e}"))?;
    keys(
        result.rows().iter().map(|r| r.values().first().cloned()),
        &query,
    )
}

async fn locy(session: &Session, predicate: &str) -> Result<Keys> {
    let program = format!(
        "CREATE RULE kept AS MATCH (m:M) WHERE {predicate} YIELD KEY m \n\
         QUERY kept RETURN m.k AS k"
    );
    let result = session
        .locy(&program)
        .await
        .map_err(|e| anyhow!("{program}: {e}"))?;
    let [CommandResult::Query(rows)] = result.command_results.as_slice() else {
        bail!("{program}: expected one QUERY result");
    };
    keys(rows.iter().map(|r| r.get("k").cloned()), &program)
}

/// The join-shaped body of the entity-resolution `MERGE`.
const SAME_TYPE_LOWER: &str = "MATCH (o:M) WHERE o.t = m.t AND o.k < m.k";

/// The body of the entity-resolution program's new-entity `MERGE`: functions
/// and a disjunction in the join condition, a pattern predicate on the body
/// alone. Checked against the answer computed here as well as against its
/// `COUNT` form: the per-row path used to fail on `toLower` of a NULL outer
/// value ("Unsupported data type NULL for function lower").
const RESOLUTION: &str = "MATCH (o:M) WHERE o.t = m.t AND NOT (o)-[:R]->() \
     AND ((toLower(o.s) ENDS WITH toLower(m.s) AND o.k <> m.k) OR o.k < m.k)";

/// `(template, body)`: every shape the rewrite takes, and every shape it must
/// leave to the per-row path. All are compared with their `COUNT` form.
const CASES: &[(&str, &str)] = &[
    // An equality and a residual: a hash join with a filter.
    ("$X", SAME_TYPE_LOWER),
    ("$NX", SAME_TYPE_LOWER),
    // No equality: a nested-loop join.
    ("$X", "MATCH (o:M) WHERE o.k < m.k"),
    ("$NX", "MATCH (o:M) WHERE o.k < m.k"),
    // A NULL on either side of the key, and of the filter, is no match.
    ("$X", "MATCH (o:M) WHERE o.t = m.t AND o.k <> m.k"),
    ("$NX", "MATCH (o:M) WHERE o.t = m.t AND o.k <> m.k"),
    ("$X", "MATCH (o:M) WHERE o.s < m.s"),
    ("$NX", "MATCH (o:M) WHERE o.s < m.s"),
    // A pattern predicate on the body alone, and a disjunction in the filter.
    (
        "$NX",
        "MATCH (o:M) WHERE o.t = m.t AND NOT (o)-[:R]->() \
         AND ((o.s ENDS WITH m.s AND o.k <> m.k) OR o.k < m.k)",
    ),
    ("$NX", RESOLUTION),
    ("$X", RESOLUTION),
    // A body conjunct of its own, and a body of two nodes.
    (
        "$X",
        "MATCH (o:M) WHERE o.k > 3 AND o.t = m.t AND o.k <> m.k",
    ),
    (
        "$NX",
        "MATCH (o:M), (p:M) WHERE p.k = o.k + 1 AND o.t = m.t AND p.t = m.t",
    ),
    // Beside other conjuncts, a pattern predicate on the outer row included.
    ("m.k > 2 AND $NX", SAME_TYPE_LOWER),
    ("NOT (m)-[:R]->() AND $X", SAME_TYPE_LOWER),
    (
        "$X AND $NX",
        "MATCH (o:M) WHERE o.t = m.t AND o.k > m.k + 2",
    ),
    // Left to the per-row path: under OR, uncorrelated, a conjunct on the
    // outer row alone, a body that names the outer node, a subquery in the
    // join condition.
    ("m.k = 0 OR $X", SAME_TYPE_LOWER),
    ("$X", "MATCH (o:M) WHERE o.k = 3"),
    ("$NX", "MATCH (o:M) WHERE m.k > 2 AND o.t = m.t"),
    ("$X", "MATCH (m)-[:R]->(o:M) WHERE o.k > m.k"),
    (
        "$NX",
        "MATCH (o:M) WHERE o.t = m.t AND o.k <> m.k AND NOT (o)-[:R]->(m)",
    ),
];

async fn check(flush: bool) -> Result<()> {
    let db = open(flush).await?;
    let session = db.session();
    for (template, body) in CASES {
        let exists = predicate(template, body, false);
        let count = predicate(template, body, true);
        let want = cypher(&session, &count).await?;
        assert_eq!(cypher(&session, &exists).await?, want, "cypher: {exists}");
        assert_eq!(locy(&session, &exists).await?, want, "locy: {exists}");
    }
    Ok(())
}

#[tokio::test]
async fn exists_join_answers_match_the_per_row_subquery_flushed() -> Result<()> {
    check(true).await
}

#[tokio::test]
async fn exists_join_answers_match_the_per_row_subquery_unflushed() -> Result<()> {
    check(false).await
}

/// The answers themselves, not only their agreement with the other path.
#[tokio::test]
async fn exists_join_answers_are_the_expected_ones() -> Result<()> {
    let db = open(true).await?;
    let session = db.session();
    let all = || 0..N;
    let same_type_lower = |m: i64| all().any(|o| t(o).is_some() && t(o) == t(m) && o < m);
    let smaller_name = |m: i64| all().any(|o| matches!((s(o), s(m)), (Some(o), Some(m)) if o < m));
    let resolution = |m: i64| {
        all().any(|o| {
            let named = matches!((s(o), s(m)), (Some(o), Some(m)) if o.ends_with(&m));
            t(o).is_some() && t(o) == t(m) && !LINKED.contains(&o) && ((named && o != m) || o < m)
        })
    };
    for (predicate, want) in [
        (
            format!("NOT EXISTS {{ {RESOLUTION} }}"),
            all().filter(|m| !resolution(*m)).collect::<Keys>(),
        ),
        (
            format!("EXISTS {{ {SAME_TYPE_LOWER} }}"),
            all().filter(|m| same_type_lower(*m)).collect(),
        ),
        (
            format!("NOT EXISTS {{ {SAME_TYPE_LOWER} }}"),
            all().filter(|m| !same_type_lower(*m)).collect(),
        ),
        (
            "NOT EXISTS { MATCH (o:M) WHERE o.k < m.k }".to_string(),
            Keys::from([0]),
        ),
        (
            "EXISTS { MATCH (o:M) WHERE o.s < m.s }".to_string(),
            all().filter(|m| smaller_name(*m)).collect(),
        ),
        (
            "NOT EXISTS { MATCH (o:M) WHERE o.s < m.s }".to_string(),
            all().filter(|m| !smaller_name(*m)).collect(),
        ),
    ] {
        assert_eq!(cypher(&session, &predicate).await?, want, "{predicate}");
    }
    Ok(())
}

/// Rows the transaction has not committed are on both sides of the join.
#[tokio::test]
async fn exists_join_reads_uncommitted_rows() -> Result<()> {
    let db = open(true).await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:M {k: 100, t: 'a'}), (:M {k: 101, t: 'z'})")
        .await?;
    let result = tx
        .query(&format!(
            "MATCH (m:M) WHERE m.k >= 100 AND NOT EXISTS {{ {SAME_TYPE_LOWER} }} RETURN m.k"
        ))
        .await?;
    let got = keys(
        result.rows().iter().map(|r| r.values().first().cloned()),
        "uncommitted",
    )?;
    // 100 has earlier `a` rows; 101 is the only `z`.
    assert_eq!(got, Keys::from([101]));
    Ok(())
}

async fn operators(session: &Session, predicate: &str) -> Result<Vec<String>> {
    let query = format!("MATCH (m:M) WHERE {predicate} RETURN m.k");
    let (_, profile) = session.query_with(&query).profile().await?;
    Ok(profile
        .runtime_stats
        .iter()
        .map(|s| s.operator.clone())
        .collect())
}

fn has(operators: &[String], name: &str) -> bool {
    operators.iter().any(|o| o.starts_with(name))
}

/// The plan is a join: a hash join when the condition holds an equality, a
/// nested-loop join when it does not.
#[tokio::test]
async fn exists_tied_by_its_where_is_planned_as_a_join() -> Result<()> {
    let db = open(true).await?;
    let session = db.session();
    let hashed = operators(&session, &format!("NOT EXISTS {{ {SAME_TYPE_LOWER} }}")).await?;
    assert!(has(&hashed, "HashJoinExec"), "{hashed:?}");
    let looped = operators(&session, "EXISTS { MATCH (o:M) WHERE o.k < m.k }").await?;
    assert!(has(&looped, "NestedLoopJoinExec"), "{looped:?}");
    Ok(())
}

/// The negative twin: a body that names the outer node keeps the per-row
/// path, and so does the `COUNT` form the answers are compared with.
#[tokio::test]
async fn exists_not_tied_by_its_where_alone_is_not_a_join() -> Result<()> {
    let db = open(true).await?;
    let session = db.session();
    for predicate in [
        "EXISTS { MATCH (m)-[:R]->(o:M) WHERE o.k > m.k }".to_string(),
        format!("COUNT {{ {SAME_TYPE_LOWER} }} = 0"),
    ] {
        let found = operators(&session, &predicate).await?;
        assert!(
            !has(&found, "HashJoinExec") && !has(&found, "NestedLoopJoinExec"),
            "{predicate}: {found:?}"
        );
    }
    Ok(())
}

/// A string function of NULL is NULL, wherever the NULL comes from: the
/// query text, a parameter, or an outer row's value inside a subquery. The
/// last used to fail the per-row subquery, which reads the outer value as a
/// literal: "Unsupported data type NULL for function lower".
#[tokio::test]
async fn string_functions_of_null_are_null() -> Result<()> {
    let db = open(false).await?;
    let session = db.session();
    for function in ["toLower", "toUpper", "trim", "lTrim", "rTrim"] {
        let literal = session
            .query(&format!("RETURN {function}(null) AS v"))
            .await?;
        assert_eq!(literal.rows()[0].values()[0], Value::Null, "{function}");
        let parameter = session
            .query_with(&format!("RETURN {function}($p) AS v"))
            .param("p", Value::Null)
            .fetch_all()
            .await?;
        assert_eq!(parameter.rows()[0].values()[0], Value::Null, "{function}");
        // Node 6 has no `s`: no other node's lowered name can equal NULL.
        let outer = cypher(
            &session,
            &format!("COUNT {{ MATCH (o:M) WHERE {function}(o.s) = {function}(m.s) }} = 0"),
        )
        .await?;
        assert_eq!(outer, Keys::from([6]), "{function}");
    }
    Ok(())
}
