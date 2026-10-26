// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Durability of the database-level Locy rule registry across restarts.
//!
//! Rules registered via `db.rules().register()` persist their source to
//! `catalog/locy_rules.json` and are recompiled on open. These tests lock in
//! the round-trip, idempotent re-registration, the orphan-strata fix,
//! removal/clear durability, backward compatibility with catalog-less
//! databases, and the load-failure policy.

// Rust guideline compliant

use anyhow::Result;
use tempfile::tempdir;
use uni_db::{DataType, Uni};

/// Builds the schema the test rules reference.
async fn apply_schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("Node")
        .property("name", DataType::String)
        .edge_type("EDGE", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;
    Ok(())
}

const REACH_RULE: &str = "CREATE RULE reach AS \
     MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b";

/// Recursively finds `catalog/locy_rules.json` under `root`.
fn find_catalog(root: &std::path::Path) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(root).ok()?.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_catalog(&path) {
                return Some(found);
            }
        } else if path.file_name().is_some_and(|n| n == "locy_rules.json") {
            return Some(path);
        }
    }
    None
}

#[tokio::test]
async fn rules_survive_close_and_reopen() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path).build().await?;
        apply_schema(&db).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
            .await?;
        tx.commit().await?;
        db.rules().register(REACH_RULE).await?;
        db.flush().await?;
    }

    {
        let db = Uni::open(&path).build().await?;
        assert!(
            db.rules().list().contains(&"reach".to_string()),
            "reach should survive restart; got {:?}",
            db.rules().list()
        );
        // And it remains invocable via a goal query.
        let result = db.session().locy("QUERY reach WHERE a.name = 'A'").await?;
        assert!(
            result.derived_rules().await.unwrap().contains_key("reach"),
            "QUERY reach should resolve after restart"
        );
    }

    Ok(())
}

#[tokio::test]
async fn duplicate_registration_is_idempotent() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path).build().await?;
        apply_schema(&db).await?;
        db.rules().register(REACH_RULE).await?;
        // Exact-duplicate registration must not grow the registry.
        db.rules().register(REACH_RULE).await?;
        assert_eq!(db.rules().count(), 1);
        db.flush().await?;
    }

    {
        let db = Uni::open(&path).build().await?;
        assert_eq!(db.rules().count(), 1, "no duplicate accumulated on reopen");
        // Re-registering at startup (the common case) stays a no-op.
        db.rules().register(REACH_RULE).await?;
        assert_eq!(db.rules().count(), 1);
    }

    Ok(())
}

#[tokio::test]
async fn reregister_same_name_supersedes_prior_source() -> Result<()> {
    // Registering a different program under the same rule name supersedes the
    // prior source rather than accumulating duplicate sources/orphan strata.
    // Registry state is a pure function of sources, so the count stays 1 and
    // removal afterwards is unambiguous (one owner per name).
    let db = Uni::in_memory().build().await?;
    apply_schema(&db).await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
        .await?;
    tx.commit().await?;

    db.rules().register(REACH_RULE).await?;
    // Same name, different body.
    db.rules()
        .register(
            "CREATE RULE reach AS \
             MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, b",
        )
        .await?;
    assert_eq!(db.rules().count(), 1, "still one rule named 'reach'");

    // The rebuilt registry is functional, and removal is unambiguous because
    // the superseded source was dropped.
    let result = db.session().locy("QUERY reach WHERE a.name = 'A'").await?;
    assert!(result.derived_rules().await.unwrap().contains_key("reach"));
    assert!(db.rules().remove("reach").await?);
    assert_eq!(
        db.rules().count(),
        0,
        "removal cleanly drops the single owner"
    );
    Ok(())
}

#[tokio::test]
async fn remove_persists_and_rejects_multi_rule_sources() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path).build().await?;
        apply_schema(&db).await?;
        db.rules().register(REACH_RULE).await?;
        db.rules()
            .register(
                "CREATE RULE other AS \
                 MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY b, KEY a",
            )
            .await?;
        assert!(db.rules().remove("reach").await?);
        assert!(!db.rules().list().contains(&"reach".to_string()));
        db.flush().await?;
    }

    {
        let db = Uni::open(&path).build().await?;
        let names = db.rules().list();
        assert!(!names.contains(&"reach".to_string()), "removal persisted");
        assert!(names.contains(&"other".to_string()), "sibling survived");
    }

    // A program defining two rules cannot have one removed in isolation.
    let db = Uni::in_memory().build().await?;
    apply_schema(&db).await?;
    db.rules()
        .register(
            "CREATE RULE r1 AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b \
             CREATE RULE r2 AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY b, KEY a",
        )
        .await?;
    let err = db.rules().remove("r1").await.unwrap_err();
    assert!(
        err.to_string().contains("shares its source program"),
        "multi-rule removal should be rejected, got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn clear_persists_empty() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path).build().await?;
        apply_schema(&db).await?;
        db.rules().register(REACH_RULE).await?;
        db.rules().clear().await?;
        assert_eq!(db.rules().count(), 0);
        db.flush().await?;
    }

    {
        let db = Uni::open(&path).build().await?;
        assert_eq!(db.rules().count(), 0, "cleared registry stays empty");
    }

    Ok(())
}

#[tokio::test]
async fn missing_catalog_opens_clean() -> Result<()> {
    // A database without catalog/locy_rules.json (older build) opens with an
    // empty registry rather than failing.
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(&path).build().await?;
        apply_schema(&db).await?;
        db.flush().await?;
    }
    assert!(
        find_catalog(dir.path()).is_none(),
        "no rule catalog should exist when no rule was registered"
    );
    let db = Uni::open(&path).build().await?;
    assert_eq!(db.rules().count(), 0);
    Ok(())
}

#[tokio::test]
async fn non_compiling_persisted_rule_fails_or_skips() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    // Seed a valid rule so the catalog file exists, then corrupt it.
    {
        let db = Uni::open(&path).build().await?;
        apply_schema(&db).await?;
        db.rules().register(REACH_RULE).await?;
        db.flush().await?;
    }
    let catalog = find_catalog(dir.path()).expect("catalog file should exist");
    let bad = r#"{"version":1,"rules":[{"source":"CREATE RULE bad AS THIS IS NOT VALID LOCY","rule_names":["bad"]}]}"#;
    std::fs::write(&catalog, bad)?;

    // Default: opening fails, naming the offending rule.
    let err = match Uni::open(&path).build().await {
        Ok(_) => panic!("open should fail on a non-compiling persisted rule"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("locy_rules.json"),
        "open should fail citing the catalog, got: {err}"
    );

    // With the escape hatch: opens, the bad rule is absent, and the file is
    // retained for recovery.
    let db = Uni::open(&path)
        .skip_invalid_locy_rules(true)
        .build()
        .await?;
    assert!(!db.rules().list().contains(&"bad".to_string()));
    let retained = std::fs::read_to_string(&catalog)?;
    assert!(
        retained.contains("THIS IS NOT VALID LOCY"),
        "skipped source must be retained in the catalog file"
    );
    Ok(())
}
