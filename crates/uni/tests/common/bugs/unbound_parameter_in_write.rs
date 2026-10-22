// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A parameter referenced but not supplied is an error, in writes as in reads.
//!
//! Reads already refused `$x` with no `x` in the parameter map. Writes stored
//! `NULL` instead: `CREATE {v: $x}` created a property-less node, and
//! `SET n.v = $x` *erased* an existing value. MERGE, `ON CREATE SET` and
//! `UNWIND .. CREATE` did the same. A NOT NULL key fed from a missing
//! parameter reported "cannot be null", which blames the data for a mistake in
//! the call. openCypher raises `ParameterMissing` (TCK `Call1.feature` [11]);
//! Neo4j raises `Neo.ClientError.Statement.ParameterMissing`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(unbound_parameter_in_write)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniError, Value};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("T")
        .property("k", DataType::Int)
        .property_nullable("v", DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:T {k: 42, v: 42})").await?;
    tx.commit().await?;
    Ok(db)
}

/// The error names the missing parameter and is the typed variant.
fn assert_parameter_missing(err: &UniError, name: &str, ctx: &str) {
    match err {
        UniError::ParameterMissing { names } => {
            assert!(
                names.iter().any(|n| n == name),
                "{ctx}: must name `{name}`, got {names:?}"
            );
        }
        other => panic!("{ctx}: expected UniError::ParameterMissing, got {other:?}"),
    }
    let text = err.to_string();
    assert!(
        text.contains(name),
        "{ctx}: message must name `{name}`: {text}"
    );
}

async fn snapshot(db: &Uni) -> Result<Vec<(i64, Value)>> {
    let result = db
        .session()
        .query("MATCH (n:T) RETURN n.k AS k, n.v AS v ORDER BY k")
        .await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| {
            (
                r.value("k").and_then(Value::as_i64).expect("k"),
                r.value("v").cloned().unwrap_or(Value::Null),
            )
        })
        .collect())
}

#[tokio::test]
async fn unbound_parameter_in_write_is_refused_and_writes_nothing() -> Result<()> {
    let db = open().await?;
    let before = snapshot(&db).await?;
    let writes = [
        "CREATE (:T {k: 1, v: $x})",
        "MATCH (n:T {k: 42}) SET n.v = $x",
        "MERGE (n:T {k: 3, v: $x})",
        "MERGE (n:T {k: 4}) ON CREATE SET n.v = $x",
        "MERGE (n:T {k: 42}) ON MATCH SET n.v = $x",
        "UNWIND [1, 2] AS i CREATE (:T {k: 10 + i, v: $x})",
        "MATCH (n:T {k: 42}) SET n += $x",
        "CREATE (:T {k: 1}) CREATE (:T {k: 2, v: $x})",
        "FOREACH (i IN [1] | CREATE (:T {k: 20 + i, v: $x}))",
    ];
    for stmt in writes {
        let tx = db.session().tx().await?;
        let err = tx
            .execute(stmt)
            .await
            .expect_err(&format!("{stmt}: a missing parameter must fail"));
        assert_parameter_missing(&err, "x", stmt);
        tx.rollback();
        assert_eq!(
            snapshot(&db).await?,
            before,
            "{stmt}: nothing may be written"
        );
    }
    // The same through `query`, which returns rows.
    let tx = db.session().tx().await?;
    let err = tx
        .query("CREATE (n:T {k: 7, v: $x}) RETURN n.v AS v")
        .await
        .expect_err("CREATE .. RETURN with a missing parameter must fail");
    assert_parameter_missing(&err, "x", "tx.query CREATE");
    tx.rollback();
    assert_eq!(
        snapshot(&db).await?,
        before,
        "tx.query: nothing may be written"
    );
    Ok(())
}

/// A NOT NULL key fed from a missing parameter names the parameter, not a null.
#[tokio::test]
async fn unbound_parameter_in_write_for_not_null_key_names_the_parameter() -> Result<()> {
    let db = open().await?;
    let tx = db.session().tx().await?;
    let err = tx
        .execute("CREATE (:T {k: $missing_key})")
        .await
        .expect_err("a missing key parameter must fail");
    assert_parameter_missing(&err, "missing_key", "NOT NULL key");
    assert!(
        !err.to_string().contains("cannot be null"),
        "the error must not blame a null value: {err}"
    );
    tx.rollback();
    Ok(())
}

/// Supplied parameters keep working, including an explicit null.
#[tokio::test]
async fn unbound_parameter_in_write_supplied_null_is_not_missing() -> Result<()> {
    let db = open().await?;
    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (n:T {k: 42}) SET n.v = $x")
        .param("x", Value::Null)
        .run()
        .await?;
    tx.execute_with("CREATE (:T {k: 5, v: $x})")
        .param("x", Value::Int(9))
        .run()
        .await?;
    tx.commit().await?;
    assert_eq!(
        snapshot(&db).await?,
        vec![(5, Value::Int(9)), (42, Value::Null)],
        "a supplied null is a value, not a missing parameter"
    );
    Ok(())
}

/// Reads raise the same typed error.
#[tokio::test]
async fn unbound_parameter_in_write_reads_raise_the_same_error() -> Result<()> {
    let db = open().await?;
    for read in [
        "RETURN $x AS x",
        "MATCH (n:T) WHERE n.v = $x RETURN n.k",
        "MATCH (n:T) RETURN n.k + $x AS s",
    ] {
        let err = db
            .session()
            .query(read)
            .await
            .expect_err(&format!("{read}: a missing parameter must fail"));
        assert_parameter_missing(&err, "x", read);
    }
    Ok(())
}

/// A Locy rule body reading a missing parameter fails the same way.
#[tokio::test]
async fn unbound_parameter_in_write_locy_rule_raises_parameter_missing() -> Result<()> {
    let db = open().await?;
    let err = db
        .session()
        .locy("CREATE RULE r AS MATCH (n:T) WHERE n.k = $x YIELD KEY n QUERY r RETURN n.k AS k")
        .await
        .expect_err("a Locy rule reading a missing parameter must fail");
    assert_parameter_missing(&err, "x", "Locy rule body");
    let err = db
        .session()
        .locy("CREATE RULE r AS MATCH (n:T) YIELD KEY n, n.k + $y AS s QUERY r RETURN s")
        .await
        .expect_err("a Locy yield reading a missing parameter must fail");
    assert_parameter_missing(&err, "y", "Locy yield");
    Ok(())
}

/// The names a `ParameterMissing` carries; panics on any other error.
fn missing_names(err: UniError, ctx: &str) -> Vec<String> {
    match err {
        UniError::ParameterMissing { names } => names,
        other => panic!("{ctx}: expected UniError::ParameterMissing, got {other:?}"),
    }
}

/// Every missing parameter of the statement is named, in order of first
/// appearance and once each, before anything runs: on a plan-cache miss and
/// hit, through a transaction, a prepared query and Locy, and for parameters
/// no row would ever evaluate. Neo4j: `Expected parameter(s): a, b`.
#[tokio::test]
async fn unbound_parameter_in_write_names_every_missing_parameter() -> Result<()> {
    let db = open().await?;
    let before = snapshot(&db).await?;

    let session = db.session();
    let err = session.query("RETURN $a, $b, $a").await.unwrap_err();
    assert_eq!(missing_names(err, "session"), ["a", "b"]);
    // A hit on the session's plan cache, for a plan a run that supplied
    // everything cached.
    session
        .query_with("RETURN $p + $q AS s")
        .param("p", 1)
        .param("q", 2)
        .fetch_all()
        .await?;
    let hits = session.metrics().plan_cache_hits;
    let err = session.query("RETURN $p + $q AS s").await.unwrap_err();
    assert_eq!(missing_names(err, "cached"), ["p", "q"]);
    assert_eq!(
        session.metrics().plan_cache_hits,
        hits + 1,
        "the second run must be a cache hit"
    );
    // Only the absent ones.
    let err = db
        .session()
        .query_with("RETURN $a, $b")
        .param("a", 1)
        .fetch_all()
        .await
        .unwrap_err();
    assert_eq!(missing_names(err, "partial"), ["b"]);
    // In a branch no row reaches and in a subquery; text in a string literal
    // is not a parameter.
    let err = db
        .session()
        .query(
            "MATCH (n:T) WHERE n.k = -1 AND n.v = $x \
             RETURN '$lit' AS s, COUNT { MATCH (m:T) WHERE m.k = $y } AS c",
        )
        .await
        .unwrap_err();
    assert_eq!(missing_names(err, "unevaluated"), ["x", "y"]);

    // A transaction write, on a plan-cache miss and on a hit for a plan a run
    // that supplied everything cached, writes nothing.
    // A failed statement leaves its transaction rollback-only, hence one
    // transaction per statement.
    let tx = session.tx().await?;
    let err = tx.execute("CREATE (:T {k: $k, v: $v})").await.unwrap_err();
    assert_eq!(missing_names(err, "tx miss"), ["k", "v"]);
    tx.rollback();
    let tx = session.tx().await?;
    tx.execute_with("CREATE (:T {k: $k, v: $v})")
        .param("k", 100)
        .param("v", 1)
        .run()
        .await?;
    tx.rollback();
    let tx = session.tx().await?;
    let err = tx.execute("CREATE (:T {k: $k, v: $v})").await.unwrap_err();
    assert_eq!(missing_names(err, "tx hit"), ["k", "v"]);
    tx.rollback();
    assert_eq!(snapshot(&db).await?, before, "nothing may be written");

    // A prepared query.
    let prepared = db.session().prepare("RETURN $a AS a, $b AS b").await?;
    let err = prepared.execute(&[]).await.unwrap_err();
    assert_eq!(missing_names(err, "prepared"), ["a", "b"]);
    let rows = prepared
        .execute(&[("a", Value::Int(1)), ("b", Value::Int(2))])
        .await?;
    assert_eq!(rows.len(), 1);

    // Locy.
    let err = db
        .session()
        .locy(
            "CREATE RULE r AS MATCH (n:T) WHERE n.k = $x YIELD KEY n \
             QUERY r WHERE n.v = $y RETURN n.k AS k",
        )
        .await
        .unwrap_err();
    assert_eq!(missing_names(err, "locy"), ["x", "y"]);
    let err = db
        .session()
        .locy_with(
            "CREATE RULE r AS MATCH (n:T) WHERE n.k = $x YIELD KEY n \
             QUERY r WHERE n.v = $y RETURN n.k AS k",
        )
        .param("y", 1)
        .run()
        .await
        .unwrap_err();
    assert_eq!(missing_names(err, "locy partial"), ["x"]);
    Ok(())
}
