// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #310: `uni.fts.query` scored every unindexed hit 0.5 in 4.1.1, so
//! ranking changed once the rows were flushed / the store reopened.
//!
//! Mirrors the report: on-disk store, a declared label whose `terms` carries
//! an `IndexType::FullText` index declared through the schema builder, about
//! 60 rows written in one transaction, then the same query (1) before any
//! flush, (2) after an explicit flush, (3) after shutdown and reopen. Each
//! stage must return the same ranking and scores, and the scores must not
//! collapse to a constant.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(issue_310)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::api::schema::IndexType;
use uni_db::{DataType, Session, Uni, Value};

const QUERY: &str = "CALL uni.fts.query('ElementVersion', 'terms', 'supplier', 50) \
                     YIELD node, score RETURN node.name AS name, score \
                     ORDER BY score DESC, name";

async fn ranked(db: &Uni) -> Result<Vec<(String, f64)>> {
    ranked_session(&db.session()).await
}

async fn ranked_session(session: &Session) -> Result<Vec<(String, f64)>> {
    let result = session.query(QUERY).await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| match (&r.values()[0], &r.values()[1]) {
            (Value::String(n), Value::Float(f)) => (n.clone(), *f),
            other => panic!("row: {other:?}"),
        })
        .collect())
}

fn index_built(db: &Uni) -> bool {
    db.indexes()
        .list(Some("ElementVersion"))
        .into_iter()
        .any(|i| i.metadata().last_built_at.is_some())
}

/// Rows shaped like the report: names that mention `supplier` in different
/// term positions and frequencies, plus unrelated filler.
fn rows() -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = [
        ("SupplierSite", "supplier site"),
        ("SupplierSite.code", "supplier site code"),
        ("SupplierSite#identity", "supplier site identity"),
        ("SupplierSite.supplier", "supplier site supplier"),
        ("SupplierName", "supplier name"),
        ("QualifiedSupplier", "qualified supplier"),
        (
            "Invoice.supplierRef",
            "invoice supplier ref amount currency due date",
        ),
        ("Supplier", "supplier"),
    ]
    .iter()
    .map(|(n, t)| (n.to_string(), t.to_string()))
    .collect();
    for i in 0..52 {
        out.push((
            format!("Filler{i}"),
            format!("customer order line item {i}"),
        ));
    }
    out
}

async fn declare(db: &Uni) -> Result<()> {
    db.schema()
        .label("ElementVersion")
        .property("uid", DataType::String)
        .property("sid", DataType::String)
        .property("kind", DataType::String)
        .property("name", DataType::String)
        .property("terms", DataType::String)
        .property("json", DataType::String)
        .index("terms", IndexType::FullText)
        .apply()
        .await?;
    Ok(())
}

fn print_stage(stage: &str, built: bool, hits: &[(String, f64)]) {
    println!("--- {stage} (index built: {built}) ---");
    for (rank, (name, score)) in hits.iter().enumerate() {
        println!("  {:>2} {name:<24} {score:.6}", rank + 1);
    }
}

fn assert_same(a: &[(String, f64)], b: &[(String, f64)], what: &str) {
    let names = |r: &[(String, f64)]| r.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();
    assert_eq!(names(a), names(b), "{what}: ranking moved");
    for ((n, x), (_, y)) in a.iter().zip(b) {
        assert!((x - y).abs() < 1e-6, "{what}: {n} scored {x} then {y}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn issue_310_fts_scores_stable_across_flush_and_reopen() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap().to_string();

    let (unflushed, flushed) = {
        let db = Uni::open(&path).build().await?;
        declare(&db).await?;
        let tx = db.session().tx().await?;
        for (i, (name, terms)) in rows().into_iter().enumerate() {
            tx.execute(&format!(
                "CREATE (:ElementVersion {{uid: 'u{i}', sid: 's{i}', kind: 'k', \
                 name: '{name}', terms: '{terms}', json: '{{}}'}})"
            ))
            .await?;
        }
        tx.commit().await?;

        // The issue is about rows the index does not cover yet, so the first
        // stage must really run before any build.
        let built_before = index_built(&db);
        let unflushed = ranked(&db).await?;
        print_stage("unflushed (L0)", built_before, &unflushed);
        assert!(
            !built_before,
            "the full-text index was built before the flush"
        );

        db.flush().await?;
        let flushed = ranked(&db).await?;
        print_stage("after flush", index_built(&db), &flushed);

        db.shutdown().await?;
        (unflushed, flushed)
    };

    let db = Uni::open(&path).build().await?;
    let reopened = ranked(&db).await?;
    print_stage("after reopen", index_built(&db), &reopened);

    assert_eq!(unflushed.len(), 8, "every supplier row must hit");
    let distinct: std::collections::BTreeSet<u64> =
        unflushed.iter().map(|(_, s)| (s * 1e6) as u64).collect();
    assert!(
        distinct.len() > 1,
        "unflushed scores collapsed to a constant: {unflushed:?}"
    );
    assert!(
        unflushed
            .iter()
            .filter(|(_, s)| (s - 0.5).abs() < 1e-9)
            .count()
            < 2,
        "unflushed hits scored the 0.5 placeholder: {unflushed:?}"
    );
    assert_same(&unflushed, &flushed, "unflushed vs flushed");
    assert_same(&flushed, &reopened, "flushed vs reopened");
    Ok(())
}

/// Schemaless text uses the same BM25 statistics as a declared, unindexed
/// property, including documents that do not match the query.
#[tokio::test(flavor = "multi_thread")]
async fn issue_310_schemaless_fts_scores_are_constant() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).build().await?;
    let tx = db.session().tx().await?;
    for (i, (name, terms)) in rows().into_iter().enumerate() {
        tx.execute(&format!(
            "CREATE (:ElementVersion {{uid: 'u{i}', name: '{name}', terms: '{terms}'}})"
        ))
        .await?;
    }
    tx.commit().await?;
    let unflushed = ranked(&db).await?;
    print_stage("schemaless unflushed", index_built(&db), &unflushed);
    db.flush().await?;
    let flushed = ranked(&db).await?;
    print_stage("schemaless after flush", index_built(&db), &flushed);
    assert_eq!(unflushed.len(), 8, "every supplier row must hit");
    let distinct: std::collections::BTreeSet<u64> =
        flushed.iter().map(|(_, s)| (s * 1e6) as u64).collect();
    assert!(
        distinct.len() > 1,
        "schemaless scores collapsed to a constant: {flushed:?}"
    );
    assert_same(&unflushed, &flushed, "schemaless flush");

    // Both an undeclared label and an undeclared property on a declared
    // label must agree with the declared, unindexed scoring path.
    for typed_text in [false, true] {
        let declared = Uni::in_memory().build().await?;
        let schema = declared
            .schema()
            .label("ElementVersion")
            .property("uid", DataType::String)
            .property("name", DataType::String);
        if typed_text {
            schema.property("terms", DataType::String).apply().await?;
        } else {
            schema.apply().await?;
        }
        let tx = declared.session().tx().await?;
        for (i, (name, terms)) in rows().into_iter().enumerate() {
            tx.execute_with("CREATE (:ElementVersion {uid: $uid, name: $name, terms: $terms})")
                .param("uid", format!("u{i}"))
                .param("name", name)
                .param("terms", terms)
                .run()
                .await?;
        }
        tx.commit().await?;
        assert_same(
            &flushed,
            &ranked(&declared).await?,
            "declared vs schemaless",
        );
        declared.flush().await?;
        assert_same(
            &flushed,
            &ranked(&declared).await?,
            "declared flushed vs schemaless",
        );
    }
    let top = db.session().query(&QUERY.replace(", 50)", ", 1)")).await?;
    assert_eq!(top.rows().len(), 1);
    assert_eq!(
        top.rows()[0].values()[0],
        Value::String(flushed[0].0.clone())
    );

    db.shutdown().await?;
    let db = Uni::open(&path).build().await?;
    assert_same(&flushed, &ranked(&db).await?, "schemaless reopen");
    Ok(())
}

/// The corpus is the latest visible version of each document, not a bag of
/// flushed versions plus L0 replacements. Fork and snapshot statistics are local.
#[tokio::test(flavor = "multi_thread")]
async fn issue_310_schemaless_fts_visible_corpus() -> Result<()> {
    let db = Uni::in_memory()
        .config(uni_db::UniConfig {
            auto_flush_interval: None,
            disable_fork_index_builder: true,
            ..Default::default()
        })
        .build()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "CREATE (:ElementVersion {name:'a', terms:'supplier'}), \
        (:ElementVersion {name:'b', terms:'supplier supplier'}), \
        (:ElementVersion {name:'c', terms:'supplier site code'}), \
        (:ElementVersion {name:'d', terms:'customer order'})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;
    let original = ranked(&db).await?;
    let snapshot = db.create_snapshot("fts-before").await?;
    let mut pinned = db.session();
    pinned.pin_to_version(&snapshot).await?;
    let fork = session.fork("fts-corpus").await?;

    let tx = session.tx().await?;
    tx.execute("MATCH (n:ElementVersion {name:'a'}) SET n.terms = 'customer'")
        .await?;
    tx.execute("MATCH (n:ElementVersion {name:'b'}) SET n.terms = null")
        .await?;
    tx.execute("MATCH (n:ElementVersion {name:'c'}) DELETE n")
        .await?;
    tx.execute("CREATE (:ElementVersion {name:'new', terms:'supplier supplier supplier'})")
        .await?;
    tx.commit().await?;
    let changed = ranked(&db).await?;
    assert_eq!(
        changed
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["new"]
    );
    assert_same(&original, &ranked_session(&pinned).await?, "pinned corpus");
    assert_same(&original, &ranked_session(&fork).await?, "fork corpus");
    db.flush().await?;
    assert_same(&changed, &ranked(&db).await?, "updates flush");
    assert_same(
        &original,
        &ranked_session(&pinned).await?,
        "pinned after flush",
    );
    assert_same(
        &original,
        &ranked_session(&fork).await?,
        "fork after primary flush",
    );

    let tx = fork.tx().await?;
    tx.execute("MATCH (n:ElementVersion {name:'a'}) SET n.terms = 42")
        .await?;
    tx.commit().await?;
    let fork_hits = ranked_session(&fork).await?;
    assert_eq!(fork_hits.len(), 2);
    assert!(fork_hits.iter().all(|(name, _)| name != "a"));
    fork.flush().await?;
    assert_same(
        &fork_hits,
        &ranked_session(&fork).await?,
        "fork update flush",
    );
    assert_same(
        &changed,
        &ranked(&db).await?,
        "fork does not affect primary",
    );

    let tx = session.tx().await?;
    tx.execute("MATCH (n:ElementVersion {name:'new'}) SET n.terms = 'customer'")
        .await?;
    let inside = tx.query(QUERY).await?;
    assert!(
        inside.rows().is_empty(),
        "transaction sees its own replacement"
    );
    tx.rollback();
    assert_same(&changed, &ranked(&db).await?, "rollback");
    Ok(())
}
