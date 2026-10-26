// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: in **schemaless** mode a property expression in `YIELD` was
//! type-inferred as `Float64`, so `derived` reported the wrong value.
//!
//! Not one of #158/#159/#160 — found by the generic QUERY/derived parity guard
//! the first time it compared key *values* rather than merely checking the
//! result was non-empty. The existing TCK scenario over this shape
//! (`evaluate/YieldValueColumns.feature`, "KEY property column without FOLD
//! projects its value in QUERY") asserts only on the QUERY surface, so it could
//! not see that `derived` disagreed — the same blind spot that let
//! `IsNotFoldQueryMatrix` 2d-3 stay vacuously green.
//!
//! ## Behaviour before the fix
//!
//! | property | schema declared | `derived`  | `QUERY` |
//! |----------|-----------------|------------|---------|
//! | string   | yes             | `'a'`      | `'a'`   |
//! | string   | **no**          | **`NULL`** | `'a'`   |
//! | int      | yes             | `7`        | `7`     |
//! | int      | **no**          | **`7.0`**  | `7`     |
//!
//! KEY-ness is irrelevant — a non-KEY property column behaves identically. With
//! a `FOLD` present *both* surfaces return `NULL`.
//!
//! ## Root cause (fixed)
//!
//! `property_arrow_type` returns `None` when the label or property is absent
//! from the schema — and schemaless means the label is genuinely absent, since
//! there is no schema-on-write for vertex labels. `infer_yield_type_rec` then
//! fell through to `infer_expr_type`, whose Property arm is `Float64`. That
//! became the projection's declared type, and because `plan_locy_project`'s
//! cross-domain guard only skips Utf8-vs-numeric, the LargeBinary (cv-encoded)
//! column was coerced through the `_cypher_to_float64` UDF, which returns
//! `None` for any non-numeric tag — hence NULL for strings, widened f64 for
//! ints.
//!
//! The fix declares `LargeBinary` when the property type is unknown, so the
//! declared and actual types match, no coercion runs, and the cv bytes are
//! decoded at read time. That behaviour was already described in the comment
//! above the inference — it had simply never been implemented. #112 fixed the
//! SLG side of this family and added `property_arrow_type`, but its repro
//! declares a schema, so the schemaless path stayed uncovered.
//!
//! **PROB columns are deliberately excluded** from the fallback: a probability
//! is numeric by definition and the complement / noisy-OR arithmetic reads it
//! as a real `Float64`, so there the coercion is correct. Applying the fallback
//! to PROB columns broke 20 `ProbabilisticComplement` TCK scenarios — pinned
//! below by `schemaless_prob_property_stays_numeric`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(schemaless_property)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Builds a graph with one `Tag {name: 'a', n: 7}`, with or without a declared
/// schema.
async fn setup(declare_schema: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if declare_schema {
        db.schema()
            .label("Tag")
            .property("name", DataType::String)
            .property("n", DataType::Int64)
            .done()
            .apply()
            .await?;
    }
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Tag {name: 'a', n: 7})").await?;
    tx.commit().await?;
    Ok(db)
}

/// Returns the single `derived` value of `column` for `rule`.
async fn derived_value(db: &Uni, program: &str, rule: &str, column: &str) -> Result<Option<Value>> {
    let result = db.session().locy(program).await?;
    let empty = vec![];
    Ok(result
        .derived_rule(rule)
        .await
        .unwrap()
        .unwrap_or(&empty)
        .first()
        .and_then(|row| row.get(column).cloned()))
}

const STRING_RULE: &str = "CREATE RULE tags AS MATCH (t:Tag) YIELD KEY t.name AS tag";
const INT_RULE: &str = "CREATE RULE nums AS MATCH (t:Tag) YIELD KEY t.n AS num";

// ---------------------------------------------------------------------------
// Green: with a declared schema the values are correct. These pin the boundary
// — the defect is specifically the schemaless path, not property projection.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schemaless_property_control_string_with_schema_is_correct() -> Result<()> {
    let db = setup(true).await?;
    assert_eq!(
        derived_value(&db, STRING_RULE, "tags", "tag").await?,
        Some(Value::String("a".to_string()))
    );
    Ok(())
}

#[tokio::test]
async fn schemaless_property_control_int_with_schema_is_correct() -> Result<()> {
    let db = setup(true).await?;
    assert_eq!(
        derived_value(&db, INT_RULE, "nums", "num").await?,
        Some(Value::Int(7))
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The defect itself.
// ---------------------------------------------------------------------------

/// A string property projected in `YIELD` used to come back as `NULL` from
/// `derived` when no schema was declared, while `QUERY` returned `'a'` for the
/// same rule in the same evaluation.
#[tokio::test]
async fn schemaless_string_property_must_not_become_null() -> Result<()> {
    let db = setup(false).await?;
    assert_eq!(
        derived_value(&db, STRING_RULE, "tags", "tag").await?,
        Some(Value::String("a".to_string())),
        "schemaless string property must survive projection into `derived`"
    );
    Ok(())
}

/// An integer property used to come back as a float. Quieter than the string
/// case but the same cause, and it changed the type users see.
#[tokio::test]
async fn schemaless_int_property_must_keep_its_type() -> Result<()> {
    let db = setup(false).await?;
    assert_eq!(
        derived_value(&db, INT_RULE, "nums", "num").await?,
        Some(Value::Int(7)),
        "schemaless integer property must not be widened to Float"
    );
    Ok(())
}

/// The two surfaces must agree, whichever value is correct. This is the
/// assertion the existing TCK scenario was missing.
#[tokio::test]
async fn schemaless_property_derived_and_query_must_agree() -> Result<()> {
    let db = setup(false).await?;
    let program = format!("{STRING_RULE}\nQUERY tags RETURN tag");
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    let derived = result
        .derived_rule("tags")
        .await
        .unwrap()
        .unwrap_or(&empty)
        .first()
        .and_then(|r| r.get("tag").cloned());
    let queried = result
        .rows()
        .unwrap_or(&empty)
        .first()
        .and_then(|r| r.get("tag").cloned());
    assert_eq!(
        derived, queried,
        "the same rule in the same evaluation must not disagree across surfaces"
    );
    Ok(())
}

/// A schemaless **PROB** property must stay numeric.
///
/// PROB columns are the one case the LargeBinary fallback must not touch: the
/// complement and noisy-OR arithmetic read the column as a real `Float64`.
/// Applying the fallback to them silently broke 20 `ProbabilisticComplement`
/// TCK scenarios — `IS NOT` complements came back wrong rather than erroring.
/// This pins the exclusion so a later simplification cannot quietly drop it.
#[tokio::test]
async fn schemaless_prob_property_stays_numeric() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {name: 'x', risk: 0.25})").await?;
    tx.commit().await?;

    let program = "CREATE RULE risky AS MATCH (i:Item) YIELD KEY i, i.risk AS r PROB\n\
         CREATE RULE safe AS MATCH (i:Item) WHERE i IS NOT risky YIELD KEY i, 1.0 AS s PROB";
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let r = result
        .derived_rule("risky")
        .await
        .unwrap()
        .unwrap_or(&empty)
        .first()
        .and_then(|row| row.get("r").cloned());
    match r {
        Some(Value::Float(f)) => assert!(
            (f - 0.25).abs() < 1e-9,
            "schemaless PROB property must keep its numeric value, got {f}"
        ),
        other => panic!("schemaless PROB column must be Float, got {other:?}"),
    }
    Ok(())
}

/// Probe for the `uni_raw_bytes` question raised while fixing the above: does a
/// **typed** `DataType::Bytes` property round-trip through Locy `derived`?
///
/// `record_batches_to_locy_rows` does not honour the `uni_raw_bytes` field
/// marker that its Cypher twin (`executor/read.rs`) checks, so raw bytes could
/// in principle be fed to the CypherValue codec and mis-decode. This test
/// records the actual behaviour rather than assuming it.
#[tokio::test]
async fn typed_bytes_property_round_trips_through_derived() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Blob")
        .property("payload", DataType::Bytes)
        .done()
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute_with("CREATE (:Blob {payload: $p})")
        .param("p", Value::Bytes(vec![1, 2, 3, 4]))
        .run()
        .await?;
    tx.commit().await?;

    let program = "CREATE RULE blobs AS MATCH (b:Blob) YIELD KEY b, b.payload AS p";
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let got = result
        .derived_rule("blobs")
        .await
        .unwrap()
        .unwrap_or(&empty)
        .first()
        .and_then(|row| row.get("p").cloned());
    assert_eq!(
        got,
        Some(Value::Bytes(vec![1, 2, 3, 4])),
        "a typed Bytes property must survive projection into `derived` intact"
    );
    Ok(())
}

/// A label that is undeclared while *other* labels are declared — the shape the
/// TCK's sidecar mode produces, and a partial schema is a realistic user state.
///
/// This is the case that caught an over-broad first version of the fix.
#[tokio::test]
async fn partially_declared_schema_undeclared_label_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Sensor")
        .property("name", DataType::String)
        .property("val", DataType::Float64)
        .done()
        .label("Node")
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Tag {name: 'a'})").await?;
    tx.commit().await?;

    let program =
        "CREATE RULE tags AS MATCH (t:Tag) YIELD KEY t.name AS tag\nQUERY tags RETURN tag";
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let derived = result
        .derived_rule("tags")
        .await
        .unwrap()
        .unwrap_or(&empty)
        .first()
        .and_then(|r| r.get("tag").cloned());
    let queried = result
        .rows()
        .unwrap_or(&empty)
        .first()
        .and_then(|r| r.get("tag").cloned());
    assert_eq!(
        derived,
        Some(Value::String("a".to_string())),
        "an undeclared label alongside declared ones must still project its value"
    );
    assert_eq!(derived, queried, "surfaces must agree");
    Ok(())
}
