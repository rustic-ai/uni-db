// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `range(start, end[, step])` with a null argument returns null.
//!
//! A null bound or step is an absent value, not a wrong type, so `range`
//! propagates it like the other scalar functions: `range(0, null)` is null and
//! `UNWIND range(0, null)` produces no rows. A non-null argument that is not
//! an integer is still a type error, and a zero step is still out of range.
//!
//! `range` has four evaluators and each one is driven here: the DataFusion UDF
//! (a scalar null literal, a null parameter and a null column value, in L0 and
//! after a flush), the `UNWIND` row evaluator, the fallback executor (a
//! standalone `CALL` of a registered procedure runs the whole statement
//! there), and the Locy command evaluator (`QUERY … RETURN`).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(range_null)'

// Rust guideline compliant

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};
use uni_query::{ProcedureOutput, ProcedureValueType, RegisteredProcedure};

/// Every null-argument shape, as Cypher argument lists over a variable `x`
/// that is null.
const NULL_ARGS: [&str; 6] = ["0, x", "x, 3", "0, 3, x", "x, x", "x, 3, 0", "0, 3.5, x"];

/// A label with a typed integer column, one row whose value is null and one
/// whose value is 3.
async fn setup(flush: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("R")
        .property_nullable("x", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:R {k: 1}), (:R {k: 2, x: 3})").await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

/// Registers `test.range_null()`, yielding one row whose `v` is null.
fn register_null_procedure(db: &Uni) {
    db.procedure_registry().register(RegisteredProcedure {
        name: "test.range_null".to_string(),
        params: Vec::new(),
        outputs: vec![ProcedureOutput {
            name: "v".to_string(),
            output_type: ProcedureValueType::Number,
        }],
        data: vec![HashMap::from([("v".to_string(), Value::Null)])],
    });
}

async fn single(db: &Uni, query: &str) -> Result<Value> {
    let rows = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{query}: {e}"))?;
    assert_eq!(rows.len(), 1, "{query}: expected one row");
    Ok(rows.rows()[0].value("r").cloned().unwrap_or(Value::Null))
}

/// DataFusion path: a null literal, a null parameter and a null bound through
/// `WITH` each make `range` null.
#[tokio::test]
async fn range_null_datafusion_literal_param_and_with() -> Result<()> {
    let db = setup(false).await?;
    for args in NULL_ARGS {
        let literal = args.replace('x', "null");
        let q = format!("RETURN range({literal}) AS r");
        assert_eq!(single(&db, &q).await?, Value::Null, "{q}");

        let q = format!("WITH null AS x RETURN range({args}) AS r");
        assert_eq!(single(&db, &q).await?, Value::Null, "{q}");

        let q = format!("UNWIND [null] AS x RETURN range({args}) AS r");
        assert_eq!(single(&db, &q).await?, Value::Null, "{q}");

        let p = args.replace('x', "$x");
        let rows = db
            .session()
            .query_with(&format!("RETURN range({p}) AS r"))
            .param("x", Value::Null)
            .fetch_all()
            .await
            .map_err(|e| anyhow::anyhow!("range({p}) with $x = null: {e}"))?;
        assert_eq!(
            rows.rows()[0].value("r").cloned().unwrap_or(Value::Int(-1)),
            Value::Null,
            "range({p}) with $x = null"
        );
    }
    Ok(())
}

/// DataFusion path over a stored column: the null row answers null and the
/// non-null row in the same batch still answers its list, in L0 and after a
/// flush.
#[tokio::test]
async fn range_null_datafusion_column() -> Result<()> {
    for flush in [false, true] {
        let db = setup(flush).await?;
        for (args, expected) in [
            ("0, n.x", vec![0, 1, 2, 3]),
            ("n.x, 4", vec![3, 4]),
            ("0, 6, n.x", vec![0, 3, 6]),
        ] {
            let q = format!("MATCH (n:R) RETURN n.k AS k, range({args}) AS r ORDER BY k");
            let rows = db.session().query(&q).await?;
            assert_eq!(rows.len(), 2, "{q}");
            assert_eq!(
                rows.rows()[0].value("r").cloned(),
                Some(Value::Null),
                "{q} flush={flush}: null row"
            );
            assert_eq!(
                rows.rows()[1].value("r").cloned(),
                Some(Value::List(expected.into_iter().map(Value::Int).collect())),
                "{q} flush={flush}: non-null row"
            );
        }
    }
    Ok(())
}

/// `UNWIND range(…)` with a null argument produces no rows, like
/// `UNWIND null`.
#[tokio::test]
async fn range_null_unwind_yields_no_rows() -> Result<()> {
    let db = setup(false).await?;
    for args in NULL_ARGS {
        let literal = args.replace('x', "null");
        for q in [
            format!("UNWIND range({literal}) AS i RETURN i"),
            format!("WITH null AS x UNWIND range({args}) AS i RETURN i"),
            format!("MATCH (n:R) WHERE n.k = 1 WITH n.x AS x UNWIND range({args}) AS i RETURN i"),
        ] {
            let rows = db.session().query(&q).await?;
            assert_eq!(rows.len(), 0, "{q}");
        }
    }
    Ok(())
}

/// Fallback executor: a standalone `CALL` runs the statement outside
/// DataFusion, and a null yielded value makes `range` null there too.
#[tokio::test]
async fn range_null_fallback_executor() -> Result<()> {
    let db = setup(false).await?;
    register_null_procedure(&db);
    for args in NULL_ARGS {
        let q = format!("CALL test.range_null() YIELD v WITH v AS x RETURN range({args}) AS r");
        assert_eq!(single(&db, &q).await?, Value::Null, "{q}");
        let q = format!(
            "CALL test.range_null() YIELD v RETURN range({}) AS r",
            args.replace('x', "v")
        );
        assert_eq!(single(&db, &q).await?, Value::Null, "{q}");
    }
    Ok(())
}

/// Locy command evaluator: `QUERY … RETURN range(…)` over a null derived
/// value is null, and over a non-null one is the list.
#[tokio::test]
async fn range_null_locy_query_return() -> Result<()> {
    let db = setup(false).await?;
    let session = db.session();
    let program = r#"
        CREATE RULE xs AS
            MATCH (n:R)
            YIELD KEY n.k AS k, n.x AS x

        QUERY xs RETURN k, range(0, x) AS r, range(x, 3) AS s, range(0, 3, x) AS t
    "#;
    let result = session.locy(program).await?;
    let rows = result.rows().expect("QUERY rows");
    assert_eq!(rows.len(), 2);
    for row in rows {
        let k = row.get("k").cloned();
        let (r, s, t) = (
            row.get("r").cloned(),
            row.get("s").cloned(),
            row.get("t").cloned(),
        );
        if k == Some(Value::Int(1)) {
            assert_eq!(
                (r, s, t),
                (Some(Value::Null), Some(Value::Null), Some(Value::Null))
            );
        } else {
            assert_eq!(k, Some(Value::Int(2)));
            let ints = |v: &[i64]| Some(Value::List(v.iter().copied().map(Value::Int).collect()));
            assert_eq!((r, s, t), (ints(&[0, 1, 2, 3]), ints(&[3]), ints(&[0, 3])));
        }
    }
    Ok(())
}

/// Unchanged: a non-null, non-integer argument is a type error and a zero
/// step is out of range, on the DataFusion path, the `UNWIND` evaluator and
/// the fallback executor.
#[tokio::test]
async fn range_null_keeps_type_and_step_errors() -> Result<()> {
    let db = setup(false).await?;
    register_null_procedure(&db);
    for (q, needle) in [
        ("RETURN range(0, 'a') AS r", "InvalidArgumentType"),
        ("RETURN range(0.5, 3) AS r", "InvalidArgumentType"),
        ("RETURN range(0, 3, 0) AS r", "NumberOutOfRange"),
        ("UNWIND range(0, 'a') AS i RETURN i", "InvalidArgumentType"),
        ("UNWIND range(0, 3, 0) AS i RETURN i", "NumberOutOfRange"),
        (
            "CALL test.range_null() YIELD v RETURN range(0, 'a') AS r",
            "InvalidArgumentType",
        ),
        (
            "CALL test.range_null() YIELD v RETURN range(0, 3, 0) AS r",
            "NumberOutOfRange",
        ),
    ] {
        let err = db
            .session()
            .query(q)
            .await
            .err()
            .unwrap_or_else(|| panic!("{q}: expected an error"));
        assert!(err.to_string().contains(needle), "{q}: {err}");
    }
    Ok(())
}
