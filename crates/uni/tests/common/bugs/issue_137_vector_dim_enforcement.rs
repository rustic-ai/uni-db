// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro/regression for issue #137.
//!
//! A declared `VECTOR(dim)` column did not enforce its dimension anywhere,
//! producing four independent silent failures:
//!   (a) a wrong-length write was accepted, then silently nulled by the Arrow
//!       converters at flush — detonating at shutdown as an unrelated-looking
//!       `UniInternalError` ("non-nullable column contains null values");
//!   (b) a wrong-length kNN query vector silently returned 0 rows instead of
//!       erroring (every candidate skipped by the length guard);
//!   (c) re-applying the schema with a different dimension was silently
//!       swallowed by the "already exists" match — the column kept its old dim
//!       while the caller believed it changed;
//!   (d) the corruption surfaced only at flush/shutdown, far from its cause.
//!
//! The fix enforces the declared dimensions at every seam: the Cypher write
//! guard (`DataType::check_vector_dims`), the writer validation used by the
//! bulk APIs, the auto-embed output, query time (`StorageManager::vector_search`),
//! re-declare (`declare_property`: idempotent only for an identical declaration),
//! and a fail-closed flush backstop.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration issue_137

use tempfile::tempdir;
use uni_common::{Properties, Value};
use uni_db::api::fork_diff::PromotePattern;
use uni_db::{DataType, Uni};

fn vec3_schema() -> DataType {
    DataType::Vector { dimensions: 3 }
}

/// (a) A wrong-length vector into a declared VECTOR(3) column is rejected at
/// CREATE with a message carrying both the declared and actual lengths.
#[tokio::test]
async fn issue_137_wrong_dim_create_rejected() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property_nullable("embedding", vec3_schema())
        .done()
        .apply()
        .await?;

    // A failed statement marks its transaction rollback-only, so each rejected
    // write gets a fresh transaction.
    let expect_write_err = |stmt: &'static str, why: &'static str| {
        let session = db.session();
        async move {
            let tx = session.tx().await.unwrap();
            let err = tx.query_with(stmt).fetch_all().await.expect_err(why);
            tx.rollback();
            err.to_string()
        }
    };

    let msg = expect_write_err(
        "CREATE (:Doc {embedding: [1.0, 2.0, 3.0, 4.0, 5.0]})",
        "5-dim vector into VECTOR(3) must be rejected at write time",
    )
    .await;
    assert!(msg.contains("TypeError"), "message: {msg}");
    assert!(
        msg.contains('3') && msg.contains('5'),
        "message must carry declared and actual lengths: {msg}"
    );

    // Empty list and non-numeric elements are rejected too (previously the
    // non-numeric elements were silently zeroed).
    let msg = expect_write_err(
        "CREATE (:Doc {embedding: []})",
        "empty list into VECTOR(3) must be rejected",
    )
    .await;
    assert!(msg.contains("TypeError"), "got: {msg}");
    let msg = expect_write_err(
        "CREATE (:Doc {embedding: [1.0, 'x', 3.0]})",
        "non-numeric element must be rejected",
    )
    .await;
    assert!(msg.contains("not numeric"), "got: {msg}");
    Ok(())
}

/// (a) SET is symmetric with CREATE.
#[tokio::test]
async fn issue_137_wrong_dim_set_rejected() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property_nullable("embedding", vec3_schema())
        .done()
        .apply()
        .await?;

    {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {embedding: [1.0, 2.0, 3.0]})")
            .await?;
        tx.commit().await?;
    }

    let tx = db.session().tx().await?;
    let err = tx
        .query_with("MATCH (d:Doc) SET d.embedding = [1.0]")
        .fetch_all()
        .await
        .expect_err("1-dim vector into VECTOR(3) via SET must be rejected");
    assert!(err.to_string().contains("TypeError"), "got: {err}");
    Ok(())
}

/// Negative control: a correct-dimension write persists across flush + reopen,
/// stays visible to kNN, and shutdown raises no internal error. Null into a
/// nullable vector column and schemaless labels stay permissive.
#[tokio::test]
async fn issue_137_correct_dim_write_persists_and_shutdown_clean() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("Doc")
            .property_nullable("embedding", vec3_schema())
            .done()
            .apply()
            .await?;

        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {embedding: [1.0, 0.0, 0.0]})")
            .await?;
        // Null into a nullable vector column is fine.
        tx.execute("CREATE (:Doc {embedding: null})").await?;
        // Schemaless labels keep accepting any length.
        tx.execute("CREATE (:FreeForm {embedding: [1.0, 2.0]})")
            .await?;
        tx.commit().await?;

        db.flush().await?;
        db.shutdown().await?; // the original repro detonated here
    }

    let db = Uni::open(&path).build().await?;
    let res = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0, 0.0], 10)
             YIELD node, score RETURN score",
        )
        .await?;
    assert_eq!(res.len(), 1, "the valid row must stay visible to kNN");
    Ok(())
}

/// (b) A wrong-length query vector errors instead of silently returning 0 rows —
/// both against unflushed L0 data and after flush.
#[tokio::test]
async fn issue_137_wrong_dim_query_errors() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property_nullable("embedding", vec3_schema())
        .done()
        .apply()
        .await?;

    {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {embedding: [1.0, 0.0, 0.0]})")
            .await?;
        tx.commit().await?;
    }

    let wrong_dim_query = "CALL uni.vector.query('Doc', 'embedding', \
                           [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 5) \
                           YIELD node, score RETURN score";

    // Pre-flush (L0 brute-force path).
    let err = db
        .session()
        .query(wrong_dim_query)
        .await
        .expect_err("8-dim query against VECTOR(3) must error, not return 0 rows");
    assert!(err.to_string().contains("dimension mismatch"), "got: {err}");

    // Post-flush (storage path).
    db.flush().await?;
    let err = db
        .session()
        .query(wrong_dim_query)
        .await
        .expect_err("8-dim query must error after flush too");
    assert!(err.to_string().contains("dimension mismatch"), "got: {err}");

    // Correct-dimension query still works.
    let res = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0, 0.0], 5)
             YIELD node, score RETURN score",
        )
        .await?;
    assert_eq!(res.len(), 1);
    Ok(())
}

/// (c) Re-applying an identical schema is idempotent; re-declaring with a
/// different dimension is a hard schema conflict and the column keeps its
/// original dimensions.
#[tokio::test]
async fn issue_137_redeclare_dim_change_rejected() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let apply_vec3 = || async {
        db.schema()
            .label("Doc")
            .property_nullable("embedding", vec3_schema())
            .done()
            .apply()
            .await
    };
    apply_vec3().await?;
    apply_vec3().await?; // identical re-apply: the register-on-every-open pattern

    let err = db
        .schema()
        .label("Doc")
        .property_nullable("embedding", DataType::Vector { dimensions: 8 })
        .done()
        .apply()
        .await
        .expect_err("re-declaring VECTOR(3) as VECTOR(8) must be a schema conflict");
    let msg = err.to_string();
    assert!(
        msg.contains('3') && msg.contains('8'),
        "conflict must name both dimensions: {msg}"
    );

    // The column still enforces the ORIGINAL dimensions.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {embedding: [1.0, 2.0, 3.0]})")
        .await?;
    let err = tx
        .query_with("CREATE (:Doc {embedding: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]})")
        .fetch_all()
        .await
        .expect_err("8-dim write must still be rejected — the re-declare was refused");
    assert!(err.to_string().contains("TypeError"), "got: {err}");
    Ok(())
}

/// The bulk insert API (which bypasses the Cypher coercion guard) rejects a
/// wrong-dimension row with its index; a clean batch succeeds.
#[tokio::test]
async fn issue_137_bulk_insert_wrong_dim_rejected() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property_nullable("embedding", vec3_schema())
        .done()
        .apply()
        .await?;

    let good = || -> Properties {
        [("embedding".to_string(), Value::Vector(vec![1.0, 2.0, 3.0]))]
            .into_iter()
            .collect()
    };
    let bad: Properties = [("embedding".to_string(), Value::Vector(vec![1.0, 2.0]))]
        .into_iter()
        .collect();

    let tx = db.session().tx().await?;
    let err = tx
        .bulk_insert_vertices("Doc", vec![good(), bad])
        .await
        .expect_err("bulk row with a 2-dim vector into VECTOR(3) must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("dimension mismatch") && msg.contains('1'),
        "error must carry the offending row index: {msg}"
    );
    tx.rollback();

    let tx = db.session().tx().await?;
    let vids = tx.bulk_insert_vertices("Doc", vec![good(), good()]).await?;
    assert_eq!(vids.len(), 2);
    tx.commit().await?;
    Ok(())
}

/// Multi-vector (`List(Vector)`) columns enforce per-token dimensions; an empty
/// token list stays a legal empty multi-vector.
#[tokio::test]
async fn issue_137_multivector_token_dims_enforced() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: 2 })),
        )
        .done()
        .apply()
        .await?;

    {
        // Valid tokens and an empty token set are fine.
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {tokens: [[1.0, 2.0], [3.0, 4.0]]})")
            .await?;
        tx.execute("CREATE (:Doc {tokens: []})").await?;
        tx.commit().await?;
    }

    // A wrong-dimension token errors, naming the token index (fresh tx: the
    // failure marks it rollback-only).
    let tx = db.session().tx().await?;
    let err = tx
        .query_with("CREATE (:Doc {tokens: [[1.0, 2.0], [9.0, 9.0, 9.0]]})")
        .fetch_all()
        .await
        .expect_err("3-dim token into List(Vector(2)) must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("token 1"), "message: {msg}");
    tx.rollback();

    // The committed rows survive a flush (nothing was silently nulled).
    db.flush().await?;
    let res = db
        .session()
        .query("MATCH (d:Doc) RETURN count(d) AS c")
        .await?;
    let count: i64 = res.rows()[0].get("c")?;
    assert_eq!(count, 2);
    Ok(())
}

/// Fork-promote sanity (risk check): vectors written on a fork are validated at
/// their Cypher write, so a promote onto primary carries only correct-dimension
/// values, flushes cleanly, and stays kNN-visible.
#[tokio::test]
async fn issue_137_fork_promote_vectors_flush_cleanly() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property_nullable("embedding", vec3_schema())
        .done()
        .apply()
        .await?;

    let session = db.session();
    {
        let fork = session.fork("staging").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Doc {embedding: [0.0, 1.0, 0.0]})")
            .await?;
        // The fork rejects wrong-dim writes too (fresh tx: failure marks it
        // rollback-only).
        tx.commit().await?;
        let tx = fork.tx().await?;
        let err = tx
            .query_with("CREATE (:Doc {embedding: [1.0, 2.0]})")
            .fetch_all()
            .await
            .expect_err("wrong-dim vector on a fork must be rejected");
        assert!(err.to_string().contains("TypeError"), "got: {err}");
        tx.rollback();
    }

    let report = db
        .promote_from_fork("staging", &[PromotePattern::label("Doc")])
        .await?;
    assert!(report.vertices_inserted >= 1, "report: {report:?}");

    // The promoted row flushes (fail-closed backstop stays quiet) and is
    // visible to kNN.
    db.flush().await?;
    let res = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [0.0, 1.0, 0.0], 5)
             YIELD node, score RETURN score",
        )
        .await?;
    assert_eq!(res.len(), 1);
    Ok(())
}

/// A vector type without a dimension is refused where it is declared, and a
/// vector index over a property with no usable dimension is refused.
#[tokio::test]
async fn vector_type_and_index_need_a_dimension() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    let err = db
        .session()
        .run("CALL uni.schema.createLabel('Bare', {properties: {emb: {type: 'VECTOR'}}})")
        .await
        .expect_err("VECTOR without a dimension must be refused");
    assert!(err.to_string().contains("VECTOR(384)"), "message: {err}");
    for bad in ["VECTOR(0)", "VECTOR(x)", "VECTOR()"] {
        let stmt = format!(
            "CALL uni.schema.createLabel('Bare', {{properties: {{emb: {{type: '{bad}'}}}}}})"
        );
        assert!(
            db.session().run(&stmt).await.is_err(),
            "{bad} must be refused"
        );
    }

    db.session()
        .run("CALL uni.schema.createLabel('Doc', {properties: {emb: {type: 'VECTOR(3)'}, tokens: {type: 'LIST<VECTOR(2)>'}}})")
        .await?;
    let schema = db.schema_manager().schema();
    let props = &schema.properties["Doc"];
    assert_eq!(props["emb"].r#type, vec3_schema());
    assert_eq!(
        props["tokens"].r#type,
        DataType::List(Box::new(DataType::Vector { dimensions: 2 }))
    );
    db.session()
        .run("CALL uni.schema.createIndex('Doc', 'emb', {type: 'VECTOR'})")
        .await?;

    // An index over a VECTOR property declared with no dimension (if the
    // builder accepts the declaration at all).
    let declared = db
        .schema()
        .label("Zero")
        .property_nullable("emb", DataType::Vector { dimensions: 0 })
        .done()
        .apply()
        .await;
    if declared.is_err() {
        return Ok(());
    }
    let err = db
        .session()
        .run("CALL uni.schema.createIndex('Zero', 'emb', {type: 'VECTOR'})")
        .await
        .expect_err("a vector index over a dimensionless VECTOR must be refused");
    assert!(err.to_string().contains("VECTOR(N)"), "message: {err}");
    Ok(())
}
