// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Type × surface matrix for Locy projections.
//!
//! Systematically asserts that every typed column kind round-trips with its
//! correct logical type across each Locy projection surface. This is the
//! standing guard against the typed-value-at-the-boundary bug class
//! (#111/#112/#113): a KEY/value column that silently collapses to `Value::Null`
//! or loses its logical type fails here even when the row count is correct — the
//! blind spot that let those bugs ship.
//!
//! Surfaces exercised per type:
//! * S1 — `YIELD KEY n.val AS v` with NO FOLD (the #112 axis)
//! * S2 — `YIELD KEY n.val AS v` WITH a trivial FOLD present
//! * S3 — `YIELD n.val AS v` as a non-key value column
//!
//! Each cell asserts the value is present, non-Null, and of the expected type.

// Rust guideline compliant

use anyhow::Result;
use uni_db::common::TemporalValue;
use uni_db::{DataType, Uni, Value};

use crate::locy::value_assert::{TypeTag, assert_column_non_null, assert_column_typed};

/// A typed-column matrix case: a column `DataType`, the Cypher literal used to
/// CREATE the value, and the `TypeTag` the projected column must round-trip to.
struct Case {
    name: &'static str,
    dtype: DataType,
    /// Cypher expression that constructs the seed value for `CREATE`.
    literal: &'static str,
    tag: TypeTag,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "int",
            dtype: DataType::Int64,
            literal: "42",
            tag: TypeTag::Int,
        },
        Case {
            name: "float",
            dtype: DataType::Float64,
            literal: "3.5",
            tag: TypeTag::Float,
        },
        Case {
            name: "string",
            dtype: DataType::String,
            literal: "'x'",
            tag: TypeTag::Str,
        },
        Case {
            name: "bool",
            dtype: DataType::Bool,
            literal: "true",
            tag: TypeTag::Bool,
        },
        Case {
            name: "datetime",
            dtype: DataType::DateTime,
            literal: "datetime('2020-01-01T00:00:00Z')",
            tag: TypeTag::DateTime,
        },
        Case {
            name: "duration",
            dtype: DataType::Duration,
            literal: "duration('P10D')",
            tag: TypeTag::Duration,
        },
        Case {
            name: "btic",
            dtype: DataType::Btic,
            literal: "btic('1985')",
            tag: TypeTag::Btic,
        },
    ]
}

async fn db_with_value(case: &Case) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("M")
        .property("val", case.dtype.clone())
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(&format!("CREATE (:M {{val: {}}})", case.literal))
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// S1: `YIELD KEY n.val AS v` with no FOLD must project the real typed value.
#[tokio::test]
async fn matrix_s1_key_no_fold_round_trips_typed() -> Result<()> {
    for case in cases() {
        let db = db_with_value(&case).await?;
        let program = "CREATE RULE r AS MATCH (n:M) YIELD KEY n.val AS v\nQUERY r RETURN v";
        let result = db.session().locy(program).await?;
        let empty = vec![];
        let rows = result.rows().unwrap_or(&empty);
        assert_column_non_null(rows, "v");
        assert_column_typed(rows, "v", case.tag);
    }
    Ok(())
}

/// S2: the same KEY column with a trivial FOLD present (the historical
/// workaround) must keep working and stay correctly typed.
#[tokio::test]
async fn matrix_s2_key_with_fold_round_trips_typed() -> Result<()> {
    for case in cases() {
        let db = db_with_value(&case).await?;
        let program = "CREATE RULE r AS MATCH (n:M) FOLD c = COUNT(*) YIELD KEY n.val AS v, c AS cnt\nQUERY r RETURN v";
        let result = db.session().locy(program).await?;
        let empty = vec![];
        let rows = result.rows().unwrap_or(&empty);
        assert_column_typed(rows, "v", case.tag);
    }
    Ok(())
}

/// S3: a non-key value column must project the real typed value (no FOLD).
#[tokio::test]
async fn matrix_s3_value_column_round_trips_typed() -> Result<()> {
    for case in cases() {
        let db = db_with_value(&case).await?;
        let program =
            "CREATE RULE r AS MATCH (n:M) YIELD KEY n AS node, n.val AS v\nQUERY r RETURN v";
        let result = db.session().locy(program).await?;
        let empty = vec![];
        let rows = result.rows().unwrap_or(&empty);
        assert_column_non_null(rows, "v");
        assert_column_typed(rows, "v", case.tag);
    }
    Ok(())
}

/// Bytes and Vector columns have no Cypher literal, so they are seeded via
/// `bulk_insert_vertices` and asserted to round-trip through a no-FOLD `YIELD
/// KEY` — covering the LargeBinary/auto-detected decode path in Locy rows.
#[tokio::test]
async fn matrix_bytes_and_vector_round_trip_typed() -> Result<()> {
    // Bytes — includes a byte > 127 to catch tagged-codec mis-decode.
    {
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("B")
            .property("b", DataType::Bytes)
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        let mut props = std::collections::HashMap::new();
        props.insert("b".to_string(), Value::Bytes(vec![1, 2, 3, 250]));
        tx.bulk_insert_vertices("B", vec![props]).await?;
        tx.commit().await?;
        let result = db
            .session()
            .locy("CREATE RULE r AS MATCH (n:B) YIELD KEY n.b AS v\nQUERY r RETURN v")
            .await?;
        let empty = vec![];
        let rows = result.rows().unwrap_or(&empty);
        assert_column_typed(rows, "v", TypeTag::Bytes);
        assert_eq!(rows[0].get("v"), Some(&Value::Bytes(vec![1, 2, 3, 250])));
    }
    // Vector
    {
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("V")
            .property("id", DataType::String)
            .vector("v", 3)
            .done()
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        let mut props = std::collections::HashMap::new();
        props.insert("id".to_string(), Value::String("a".into()));
        props.insert("v".to_string(), Value::Vector(vec![1.0, 2.0, 3.0]));
        tx.bulk_insert_vertices("V", vec![props]).await?;
        tx.commit().await?;
        let result = db
            .session()
            .locy("CREATE RULE r AS MATCH (n:V) YIELD KEY n.id AS id, n.v AS vec\nQUERY r RETURN id, vec")
            .await?;
        let empty = vec![];
        let rows = result.rows().unwrap_or(&empty);
        assert_column_typed(rows, "vec", TypeTag::Vector);
    }
    Ok(())
}

/// Sanity: the `TemporalValue` import is exercised so the test documents the
/// concrete duration/btic shapes the matrix expects.
#[allow(dead_code, reason = "documents the expected temporal shapes")]
fn _expected_shapes() -> Vec<Value> {
    vec![
        Value::Temporal(TemporalValue::Duration {
            months: 0,
            days: 10,
            nanos: 0,
        }),
        Value::Temporal(TemporalValue::Btic {
            lo: 0,
            hi: 0,
            meta: 0,
        }),
    ]
}

// ---------------------------------------------------------------------------
// Schemaless arm.
//
// Every arm above asserts only `result.rows()` — the QUERY surface. That is
// exactly the blind spot that let the schemaless defect ship: `derived`
// returned NULL for strings and floats for ints while QUERY was correct, and no
// test compared them. These arms assert BOTH surfaces, and do it without a
// declared schema, where the property type is unknown at plan time and the
// column arrives cv-encoded as LargeBinary.
// ---------------------------------------------------------------------------

/// Seeds the same value with **no schema declared**, so the property lands in
/// `overflow_json` and its type is unknown to the planner.
async fn db_with_value_schemaless(case: &Case) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(&format!("CREATE (:M {{val: {}}})", case.literal))
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// S4: schemaless KEY property column, asserted on the `derived` surface.
#[tokio::test]
async fn matrix_s4_schemaless_key_round_trips_typed_in_derived() -> Result<()> {
    for case in cases() {
        let db = db_with_value_schemaless(&case).await?;
        let program = "CREATE RULE r AS MATCH (n:M) YIELD KEY n.val AS v";
        let result = db.session().locy(program).await?;
        let empty = vec![];
        let derived = result.derived_rule("r").await.unwrap().unwrap_or(&empty);
        assert_column_non_null(derived, "v");
        assert_column_typed(derived, "v", case.tag);
    }
    Ok(())
}

/// S5: the same schemaless column must agree across `derived` and `QUERY`.
///
/// This is the assertion whose absence hid the defect — each surface was
/// individually plausible, and only comparing them revealed the disagreement.
#[tokio::test]
async fn matrix_s5_schemaless_derived_and_query_agree() -> Result<()> {
    for case in cases() {
        let db = db_with_value_schemaless(&case).await?;
        let program = "CREATE RULE r AS MATCH (n:M) YIELD KEY n.val AS v\nQUERY r RETURN v";
        let result = db.session().locy(program).await?;
        let empty = vec![];
        let derived = result
            .derived_rule("r")
            .await
            .unwrap()
            .unwrap_or(&empty)
            .first()
            .and_then(|row| row.get("v").cloned());
        let queried = result
            .rows()
            .unwrap_or(&empty)
            .first()
            .and_then(|row| row.get("v").cloned());
        assert_eq!(
            derived, queried,
            "{}: schemaless column must agree across derived and QUERY",
            case.name
        );
    }
    Ok(())
}

/// S6: the schema'd arms above only check QUERY; assert `derived` too, so a
/// regression on either surface alone is caught.
#[tokio::test]
async fn matrix_s6_schemad_key_round_trips_typed_in_derived() -> Result<()> {
    for case in cases() {
        let db = db_with_value(&case).await?;
        let program = "CREATE RULE r AS MATCH (n:M) YIELD KEY n.val AS v";
        let result = db.session().locy(program).await?;
        let empty = vec![];
        let derived = result.derived_rule("r").await.unwrap().unwrap_or(&empty);
        assert_column_non_null(derived, "v");
        assert_column_typed(derived, "v", case.tag);
    }
    Ok(())
}

/// S7: **`LargeUtf8`-producing expressions** — the arm the matrix was missing.
///
/// The cases above all project a stored *property*, whose Arrow type comes from
/// the declared schema (`Utf8`) or from cv-encoded storage (`LargeBinary`).
/// Neither reaches `LargeUtf8`, which is what `infer_expr_type` assigns to
/// string literals and to every string-returning function — and which had no
/// decode arm at all, so all of them silently produced NULL in `derived`.
///
/// A matrix keyed on *storage* types could never have caught it; the gap was
/// only reachable through *expression* types. That is the generalizable lesson
/// and the reason this arm exists.
#[tokio::test]
async fn matrix_s7_large_utf8_expressions_round_trip_on_both_surfaces() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("M")
        .property("val", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:M {val: 'x'})").await?;
    tx.commit().await?;

    for expr in [
        "'lit'",
        "toUpper(n.val)",
        "toLower('ABC')",
        "toString(1)",
        "substring('abcdef', 1, 2)",
        "trim('  s  ')",
    ] {
        let program =
            format!("CREATE RULE r AS MATCH (n:M) YIELD KEY n, {expr} AS v\nQUERY r RETURN v");
        let result = db.session().locy(&program).await?;
        let empty = vec![];

        let derived = result.derived_rule("r").await.unwrap().unwrap_or(&empty);
        assert_column_non_null(derived, "v");
        assert_column_typed(derived, "v", TypeTag::Str);

        let queried = result.rows().unwrap_or(&empty);
        assert_column_typed(queried, "v", TypeTag::Str);
        assert_eq!(
            derived.first().and_then(|r| r.get("v")),
            queried.first().and_then(|r| r.get("v")),
            "{expr}: derived and QUERY must agree"
        );
    }
    Ok(())
}
