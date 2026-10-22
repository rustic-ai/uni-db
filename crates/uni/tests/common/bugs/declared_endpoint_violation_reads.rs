// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Edges that violate their type's declared endpoints stay readable.
//!
//! With `R2` declared `A -> F`, a store holding `(:A)-[:R2]->(:S)` and
//! `(:S)-[:R2]->(:F)` answered `MATCH (a:A)-[:R2]->(b)` without the `S` target
//! and `MATCH ()-[r:R2]->() RETURN count(r)` short. The planner narrowed an
//! unlabelled endpoint to the declared label -- the target through an inferred
//! label check, the anonymous source through a label scan -- on the claim that
//! storage could not hold anything else. Writes now enforce the declaration,
//! but a store written before that can hold violating edges, so the narrowing
//! must not drop them: a pattern that does not name a label does not filter by
//! one.
//!
//! The violating store is built the way it arises in practice -- written under
//! a wider declaration than the one now in the catalog -- by narrowing the
//! persisted declaration between two opens.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(declared_endpoint_violation_reads)'

// Rust guideline compliant

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use uni_db::{DataType, Uni, Value};

fn find_schema_json(root: &Path) -> Option<PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).ok()?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|n| n == "schema.json")
                && path
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|n| n == "catalog")
            {
                return Some(path);
            }
        }
    }
    None
}

/// Rewrite the persisted declaration of `edge_type` to `src -> dst`, as a
/// catalog written before endpoints were enforced holds it.
///
/// Such a catalog has no `endpoint_conformance` record, so the record is
/// removed too: keeping the `enforced` the first open wrote would describe a
/// catalog no release ever produced -- one claiming conformance over edges
/// that violate it.
pub(crate) fn narrow_declaration(
    root: &Path,
    edge_type: &str,
    src: &[&str],
    dst: &[&str],
) -> Result<()> {
    let path = find_schema_json(root).context("catalog/schema.json not found")?;
    let mut schema: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    let meta = schema
        .get_mut("edge_types")
        .and_then(|t| t.get_mut(edge_type))
        .context("edge type missing from schema.json")?;
    meta["src_labels"] = serde_json::json!(src);
    meta["dst_labels"] = serde_json::json!(dst);
    if let Some(fields) = meta.as_object_mut() {
        fields.remove("endpoint_conformance");
    }
    std::fs::write(&path, serde_json::to_vec_pretty(&schema)?)?;
    Ok(())
}

/// A store whose `R2` edges were written under `A|S -> F|S` and whose catalog
/// now declares `A -> F`: one conforming edge, one with a violating target,
/// one with a violating source.
pub(crate) async fn violating_store(dir: &Path) -> Result<Uni> {
    let uri = dir.to_str().context("utf-8 path")?.to_string();
    {
        let db = Uni::open(uri.clone()).build().await?;
        db.schema()
            .label("A")
            .property("v", DataType::Int)
            .label("F")
            .property("v", DataType::Int)
            .label("S")
            .property("v", DataType::Int)
            .edge_type("R2", &["A", "S"], &["F", "S"])
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute(
            "CREATE (a:A {v: 1}), (f:F {v: 2}), (s:S {v: 3}), (s2:S {v: 4}), (f2:F {v: 5}), \
                    (a)-[:R2]->(f), (a)-[:R2]->(s), (s2)-[:R2]->(f2)",
        )
        .await?;
        tx.commit().await?;
        db.shutdown().await?;
    }
    narrow_declaration(dir, "R2", &["A"], &["F"])?;
    let db = Uni::open(uri).build().await?;
    let declared = db.schema().current();
    let meta = declared.edge_types.get("R2").context("R2 declared")?;
    assert_eq!(
        meta.src_labels,
        vec!["A".to_string()],
        "the narrowed catalog is in force"
    );
    assert_eq!(
        meta.dst_labels,
        vec!["F".to_string()],
        "the narrowed catalog is in force"
    );
    Ok(db)
}

pub(crate) async fn ints(db: &Uni, cypher: &str) -> Result<Vec<i64>> {
    let result = db
        .session()
        .query(cypher)
        .await
        .map_err(|e| anyhow::anyhow!("{cypher}: {e}"))?;
    let mut out: Vec<i64> = result
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::Int(i) => *i,
            other => panic!("{cypher}: expected an integer, got {other:?}"),
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

pub(crate) async fn assert_all_edges_visible(db: &Uni, phase: &str) -> Result<()> {
    let cases: &[(&str, &[i64])] = &[
        // Anonymous endpoints, both directions: every edge.
        ("MATCH ()-[r:R2]->() RETURN count(r)", &[3]),
        ("MATCH ()<-[r:R2]-() RETURN count(r)", &[3]),
        ("MATCH ()-[r:R2]-() RETURN count(r)", &[6]),
        // Unlabelled target: the S target is not dropped.
        ("MATCH (a:A)-[:R2]->(b) RETURN b.v", &[2, 3]),
        ("MATCH (a:A)-[:R2*1..1]->(b) RETURN b.v", &[2, 3]),
        ("MATCH (a {v: 1})-[:R2]->(b) RETURN b.v", &[2, 3]),
        // Unlabelled source: the S source is not dropped.
        ("MATCH (x)-[:R2]->(b:F) RETURN x.v", &[1, 4]),
        ("MATCH (x)-[:R2]->(b) RETURN x.v", &[1, 1, 4]),
        ("MATCH (b:F)<-[:R2]-(x) RETURN x.v", &[1, 4]),
        ("MATCH (b)<-[:R2]-(x:A) RETURN b.v", &[2, 3]),
        // Labels written by the user still filter.
        ("MATCH (x:S)-[:R2]->(b) RETURN b.v", &[5]),
        ("MATCH (a:A)-[:R2]->(b:S) RETURN b.v", &[3]),
    ];
    for (cypher, want) in cases {
        assert_eq!(ints(db, cypher).await?, want.to_vec(), "{phase}: {cypher}");
    }
    Ok(())
}

#[tokio::test]
async fn declared_endpoint_violation_reads_unflushed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = violating_store(dir.path()).await?;
    assert_all_edges_visible(&db, "replayed L0").await
}

#[tokio::test]
async fn declared_endpoint_violation_reads_flushed_and_compacted() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = violating_store(dir.path()).await?;
    db.flush().await?;
    assert_all_edges_visible(&db, "flushed").await?;
    db.session()
        .run("CALL uni.admin.compact() YIELD semantic_passes RETURN semantic_passes")
        .await?;
    assert_all_edges_visible(&db, "compacted").await?;
    // A fresh process warms adjacency from storage alone.
    let uri = dir.path().to_str().context("utf-8 path")?.to_string();
    db.shutdown().await?;
    let db = Uni::open(uri).build().await?;
    assert_all_edges_visible(&db, "reopened").await
}
