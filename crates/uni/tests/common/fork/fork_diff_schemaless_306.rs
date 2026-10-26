// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #306 repro: fork diff / promote over schemaless labels and edge types.
//!
//! `compute_diff` enumerated labels and edge types from the *schema catalog*
//! only, so labels/types that exist purely in data (schemaless mode) were never
//! scanned and the diff came back empty.

use anyhow::Result;
use uni_db::api::fork_diff::PromotePattern;
use uni_db::{DataType, Uni};

/// Primary: 50 `:Concept` nodes in a `:SUBTYPE_OF` chain, flushed. Fork: add
/// one node + edge, edit one property. Returns (vertices added, deleted,
/// edges added, deleted, fork node count, primary node count).
async fn run(declared: bool) -> Result<(usize, usize, usize, usize, i64, i64)> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    if declared {
        db.schema()
            .label("Concept")
            .property("sid", DataType::String)
            .property_nullable("note", DataType::String)
            .edge_type("SUBTYPE_OF", &["Concept"], &["Concept"])
            .apply()
            .await?;
    }
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("UNWIND range(0, 49) AS i CREATE (:Concept {sid: 'C' + toString(i)})")
        .await?;
    tx.execute(
        "UNWIND range(0, 48) AS i MATCH (a:Concept {sid: 'C' + toString(i + 1)}), \
         (b:Concept {sid: 'C' + toString(i)}) CREATE (a)-[:SUBTYPE_OF]->(b)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let fork_n;
    {
        let fork = session.fork("wc-alice").await?;
        let tx = fork.tx().await?;
        tx.execute(
            "MATCH (b:Concept {sid: 'C49'}) CREATE (:Concept {sid: 'X'})-[:SUBTYPE_OF]->(b)",
        )
        .await?;
        tx.execute("MATCH (c:Concept {sid: 'C10'}) SET c.note = 'edited'")
            .await?;
        tx.commit().await?;
        fork_n = fork
            .query("MATCH (n:Concept) RETURN count(n) AS c")
            .await?
            .rows()[0]
            .get::<i64>("c")?;
    }
    let prim_n = session
        .query("MATCH (n:Concept) RETURN count(n) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;

    let diff = db.diff_fork_primary("wc-alice").await?;
    let out = (
        diff.vertices.added.len(),
        diff.vertices.deleted.len(),
        diff.edges.added.len(),
        diff.edges.deleted.len(),
        fork_n,
        prim_n,
    );
    db.shutdown().await?;
    Ok(out)
}

#[tokio::test]
async fn issue_306_diff_fork_primary_schemaless_vs_declared() -> Result<()> {
    let declared = run(true).await?;
    let schemaless = run(false).await?;
    println!("declared   (v+, v-, e+, e-, fork_n, prim_n) = {declared:?}");
    println!("schemaless (v+, v-, e+, e-, fork_n, prim_n) = {schemaless:?}");
    // Control: the fork genuinely differs from primary in both modes.
    assert_eq!((declared.4, declared.5), (51, 50));
    assert_eq!((schemaless.4, schemaless.5), (51, 50));
    // Expected: the schemaless diff reports the same changes as the declared one.
    assert_eq!(
        schemaless, declared,
        "schemaless diff must report the same changes as the declared-label diff"
    );
    Ok(())
}

/// Promote side: a schemaless label was refused with `LabelNotFound` even
/// though primary holds a row of it, and incidental schemaless edges on the
/// fork were not counted in `edges_skipped`.
#[tokio::test]
async fn issue_306_promote_from_fork_schemaless() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Thing {k: 1})").await?;
    tx.commit().await?;
    db.flush().await?;
    {
        let fork = session.fork("f").await?;
        let tx = fork.tx().await?;
        tx.execute("MATCH (t:Thing {k: 1}) CREATE (t)-[:LINK]->(:Thing {k: 2})")
            .await?;
        tx.commit().await?;
    }

    let res = db
        .promote_from_fork("f", &[PromotePattern::label("Thing")])
        .await;
    println!("promote(label Thing) on schemaless label -> {res:?}");
    let prim_n = session
        .query("MATCH (n:Thing) RETURN count(n) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    println!("primary :Thing count after promote = {prim_n} (expected 2)");
    let report = res?;
    assert_eq!(prim_n, 2, "fork-only :Thing {{k:2}} must reach primary");
    assert_eq!(
        report.edges_skipped, 1,
        "the fork's :LINK edge must be reported as skipped, got {report:?}"
    );
    db.shutdown().await?;
    Ok(())
}

/// Promote of a schemaless label with no fork-only edge type in play, so the
/// fork flush cannot be what fails: isolates the label-presence gate.
#[tokio::test]
async fn issue_306_promote_schemaless_label_vertices_only() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Thing {k: 1})").await?;
    tx.commit().await?;
    db.flush().await?;
    {
        let fork = session.fork("f").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Thing {k: 2})").await?;
        tx.commit().await?;
    }
    let res = db
        .promote_from_fork("f", &[PromotePattern::label("Thing")])
        .await;
    println!("promote(label Thing), vertices only -> {res:?}");
    let prim_n = session
        .query("MATCH (n:Thing) RETURN count(n) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    println!("primary :Thing count after promote = {prim_n} (expected 2)");
    res?;
    assert_eq!(prim_n, 2);
    db.shutdown().await?;
    Ok(())
}

/// Localizes the promote failure above: flushing a fork that holds a
/// schemaless edge type first seen on that fork. Arm `on_primary_first`
/// interns `:LINK` on primary before the fork is created (control).
async fn fork_flush_new_schemaless_edge(
    on_primary_first: bool,
    reopen_before_flush: bool,
) -> Result<String> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Thing {k: 1})").await?;
    if on_primary_first {
        tx.execute("CREATE (:Thing {k: 0})-[:LINK]->(:Thing {k: -1})")
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    let fork = session.fork("f").await?;
    let tx = fork.tx().await?;
    tx.execute("MATCH (t:Thing {k: 1}) CREATE (t)-[:LINK]->(:Thing {k: 2})")
        .await?;
    tx.commit().await?;
    let fork = if reopen_before_flush {
        // Drop the only session on the fork, then open a fresh one: this is
        // exactly what `promote_from_fork` does before its `fork.flush()`.
        drop(fork);
        session.fork("f").await?
    } else {
        fork
    };
    Ok(format!("{:?}", fork.flush().await))
}

#[tokio::test]
async fn issue_306_fork_flush_with_fork_only_schemaless_edge_type() -> Result<()> {
    let mut results = Vec::new();
    for (on_primary_first, reopen) in [(true, false), (true, true), (false, false), (false, true)] {
        let r = fork_flush_new_schemaless_edge(on_primary_first, reopen).await?;
        println!(
            "fork.flush(): :LINK interned on primary first={on_primary_first}, \
             reopened fork session={reopen} -> {r}"
        );
        results.push(r);
    }
    assert!(
        results.iter().all(|r| r == "Ok(())"),
        "every fork flush must succeed, got {results:?}"
    );
    Ok(())
}
