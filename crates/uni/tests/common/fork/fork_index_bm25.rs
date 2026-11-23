// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 5b — fork-local BM25 / FTS fusion.
//!
//! Confirms `Session::build_fork_local_index(label, column, FullText)`
//! builds a Lance native FTS (Inverted) index on the fork's branch,
//! and that FTS queries on the forked session return results
//! including both primary-inherited and fork-local rows via Lance's
//! `base_paths` chain.

// Rust guideline compliant

use std::time::Duration;

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::{IndexType, Uni};
use uni_store::fork::ForkLocalIndexKind;

#[tokio::test]
async fn fork_local_fts_index_returns_fused_results() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("body", DataType::String)
        .apply()
        .await?;

    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Doc {title: 'P-zebra', body: 'a primary article about zebras'})")
        .await?;
    tx.execute("CREATE (:Doc {title: 'P-other', body: 'a primary article about something else'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("scenario").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Doc {title: 'F-zebra', body: 'a fork-only zebra story'})")
        .await?;
    tx.commit().await?;
    forked.flush().await?;

    forked
        .build_fork_local_index("Doc", "body", ForkLocalIndexKind::FullText)
        .await?;

    // Phase 5b followup: planner emission for FTS. Explain should
    // show FusedIndexScanWrapped { kind: Bm25Rrf } wrapping the
    // original `uni.fts.query` procedure call.
    let plan = forked
        .query_with(
            "CALL uni.fts.query('Doc', 'body', 'zebra', 10)
             YIELD node, score
             RETURN node.title AS title",
        )
        .explain()
        .await?;
    assert!(
        plan.plan_text.contains("FusedIndexScanWrapped"),
        "expected FusedIndexScanWrapped after FullText registration; got {}",
        plan.plan_text
    );
    assert!(
        plan.plan_text.contains("Bm25Rrf"),
        "expected Bm25Rrf fusion kind; got {}",
        plan.plan_text
    );

    // Use the existing CALL-style FTS surface. For Phase 5b the
    // fusion comes from BranchedBackend routing through the fork's
    // branch — the surface is unchanged.
    let res = forked
        .query(
            "CALL uni.fts.query('Doc', 'body', 'zebra', 10)
             YIELD node, score
             RETURN node.title AS title, score
             ORDER BY score DESC",
        )
        .await?;

    let titles: Vec<String> = res
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("title").ok())
        .collect();

    assert!(
        titles.iter().any(|t| t == "F-zebra"),
        "fork-local zebra doc should be in results; got {titles:?}"
    );
    assert!(
        titles.iter().any(|t| t == "P-zebra"),
        "primary-inherited zebra doc should be in results; got {titles:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M6: a fork FTS query over a branch-local index must honor the
/// `_deleted = false` filter. Before the fix the branch search dropped the
/// filter, so a vertex soft-deleted on the parent *before* forking (and
/// inherited via `base_paths`) leaked back into fork results.
#[tokio::test]
async fn fork_local_fts_honors_deleted_filter() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("body", DataType::String)
        .apply()
        .await?;

    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Doc {title: 'keep', body: 'a zebra that stays'})")
        .await?;
    tx.execute("CREATE (:Doc {title: 'gone', body: 'a zebra to be deleted'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Soft-delete one zebra doc on PRIMARY, before forking, then flush so
    // the tombstone lands in the dataset the fork will branch from.
    let tx = primary.tx().await?;
    tx.execute("MATCH (d:Doc {title: 'gone'}) DETACH DELETE d")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("m6_fts").await?;
    // Write a fork-local row so the branch exists and the fork-local FTS
    // index has fork content to index alongside the inherited rows.
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Doc {title: 'fork', body: 'a fork-only zebra'})")
        .await?;
    tx.commit().await?;
    forked.flush().await?;

    forked
        .build_fork_local_index("Doc", "body", ForkLocalIndexKind::FullText)
        .await?;

    let res = forked
        .query(
            "CALL uni.fts.query('Doc', 'body', 'zebra', 10)
             YIELD node, score
             RETURN node.title AS title",
        )
        .await?;
    let titles: Vec<String> = res
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("title").ok())
        .collect();

    assert!(
        !titles.iter().any(|t| t == "gone"),
        "soft-deleted (inherited) zebra doc leaked into fork FTS results (M6); got {titles:?}"
    );
    assert!(
        titles.iter().any(|t| t == "keep"),
        "live inherited zebra doc should still be returned; got {titles:?}"
    );
    assert!(
        titles.iter().any(|t| t == "fork"),
        "fork-local zebra doc should be returned; got {titles:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M7: the background index builder must auto-build fork-local
/// vector/FTS indexes, not just scalar ones. Without it, fork rows written
/// after branch-creation are unindexed and — since FTS has no brute-force
/// fallback — silently omitted from fork results.
#[tokio::test]
async fn fork_local_fts_auto_built_for_new_rows() -> Result<()> {
    // Index builder ENABLED (default); short interval + threshold 1 so a
    // single fork fragment triggers an auto-build promptly.
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        fork_index_builder_interval: Duration::from_millis(100),
        fork_index_build_threshold: 1,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("body", DataType::String)
        .index("body", IndexType::FullText)
        .apply()
        .await?;

    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Doc {body: 'a primary zebra'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("m7_fts").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Doc {body: 'a fork zebra'})").await?;
    tx.commit().await?;
    forked.flush().await?;

    // Do NOT manually build a fork-local index. Poll: once the background
    // builder schedules the FTS build, the fork-local row becomes matchable.
    let mut saw_fork = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let res = forked
            .query(
                "CALL uni.fts.query('Doc', 'body', 'zebra', 10)
                 YIELD node, score
                 RETURN node.body AS body",
            )
            .await?;
        let bodies: Vec<String> = res
            .rows()
            .iter()
            .filter_map(|r| r.get::<String>("body").ok())
            .collect();
        if bodies.iter().any(|b| b == "a fork zebra") {
            assert!(
                bodies.iter().any(|b| b == "a primary zebra"),
                "primary-inherited match should also be present; got {bodies:?}"
            );
            saw_fork = true;
            break;
        }
    }
    assert!(
        saw_fork,
        "fork-local FTS index was not auto-built; new fork row never matched (M7)"
    );

    db.shutdown().await?;
    Ok(())
}

/// `(title, score)` for 'zebra story', best first, from `session`.
async fn zebra_ranking(session: &uni_db::Session) -> Result<Vec<(String, f64)>> {
    let res = session
        .query(
            "CALL uni.fts.query('Doc', 'body', 'zebra story', 10)
             YIELD node, score
             RETURN node.title AS title, score",
        )
        .await?;
    Ok(res
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
            )
        })
        .collect())
}

/// A fork's full-text ranking does not move when the fork flushes.
///
/// The fork's search routes through its branch
/// (`ForkBranching::full_text_search_with_unstored_on_branch`), which scores
/// the fork's unflushed rows as its branch search will once they are stored:
/// index-covered rows on the index's statistics, the rest on those plus their
/// own. Checked twice: with the branch fully indexed, then with fork rows the
/// branch index does not cover.
#[tokio::test]
async fn fork_fts_ranking_survives_a_fork_flush() -> Result<()> {
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("body", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    for i in 0..10 {
        tx.execute(&format!(
            "CREATE (:Doc {{title: 'P{i}', body: 'filler text about item {i}'}})"
        ))
        .await?;
    }
    tx.execute("CREATE (:Doc {title: 'P-zebra', body: 'a primary zebra story story'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("ranking").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Doc {title: 'F1', body: 'a fork zebra'})")
        .await?;
    tx.commit().await?;
    forked.flush().await?;
    forked
        .build_fork_local_index("Doc", "body", ForkLocalIndexKind::FullText)
        .await?;

    for (stage, rows) in [
        (
            "indexed branch",
            "(:Doc {title: 'F2', body: 'zebra story'})",
        ),
        (
            "uncovered fork rows",
            "(:Doc {title: 'F3', body: 'one long zebra story with many other words'})",
        ),
    ] {
        let tx = forked.tx().await?;
        tx.execute(&format!("CREATE {rows}")).await?;
        tx.commit().await?;
        let before = zebra_ranking(&forked).await?;
        forked.flush().await?;
        let after = zebra_ranking(&forked).await?;
        let titles = |r: &[(String, f64)]| r.iter().map(|(t, _)| t.clone()).collect::<Vec<_>>();
        assert_eq!(
            titles(&before),
            titles(&after),
            "{stage}: {before:?} vs {after:?}"
        );
        for ((title, b), (_, a)) in before.iter().zip(&after) {
            assert!((b - a).abs() < 1e-6, "{stage}: {title} scored {b} then {a}");
        }
    }

    db.shutdown().await?;
    Ok(())
}
