// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `MATCH (x)-[r:R]->(y)` with an unconstrained source reads the source from
//! `R`'s adjacency (`EdgeSourceScanExec`) instead of every vertex, and must
//! return exactly the rows the plain all-vertex traversal returns.
//!
//! Every comparison runs each query twice: as written, which takes the
//! edge-driven source, and with `WHERE id(x) >= 0` added, which holds for every
//! vertex but gives the source scan a filter, so the planner keeps the plain
//! all-vertex scan. Both arms' plans are asserted, so a comparison cannot pass
//! because both took the same path, and each scenario checks the edge count,
//! so it cannot pass because both are empty.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(edge_source_scan)'

// Rust guideline compliant

use std::collections::HashMap;

use anyhow::{Context, Result};
use uni_db::{DataType, EdgeData, Session, Transaction, Uni, UniConfig, Value};

use crate::bugs::declared_endpoint_violation_reads::narrow_declaration;
use crate::plan_shape::assert_plan_avoids;
use crate::plan_shape::assert_plan_uses;

const EDGE_SOURCE: &str = "EdgeSourceScanExec";

/// Where a query runs: a session, or inside a transaction.
#[derive(Clone, Copy)]
enum Reader<'a> {
    Session(&'a Session),
    Tx(&'a Transaction),
}

impl Reader<'_> {
    /// Canonical rows (sorted, maps key-ordered) and physical operator names.
    async fn run(self, cypher: &str) -> Result<(Vec<String>, Vec<String>)> {
        let (rows, ops, _) = self.run_counted(cypher).await?;
        Ok((rows, ops))
    }

    /// [`Self::run`], plus the rows `EdgeSourceScanExec` emitted, if it ran.
    async fn run_counted(self, cypher: &str) -> Result<(Vec<String>, Vec<String>, Option<usize>)> {
        let (result, profile) = match self {
            Reader::Session(s) => s.query_with(cypher).profile().await,
            Reader::Tx(t) => t.query_with(cypher).profile().await,
        }
        .with_context(|| cypher.to_string())?;
        let mut rows: Vec<String> = result
            .rows()
            .iter()
            .map(|row| canonical(row.values()))
            .collect::<Result<_>>()?;
        rows.sort();
        let sources = profile
            .runtime_stats
            .iter()
            .filter(|st| st.operator == EDGE_SOURCE)
            .map(|st| st.actual_rows)
            .reduce(|a, b| a + b);
        Ok((rows, uni_query::plan_shape::op_names(&profile), sources))
    }
}

fn canonical(values: &[Value]) -> Result<String> {
    // `serde_json::Map` is key-ordered, so a node's property map compares
    // equal whatever order the engine built it in.
    let json: Vec<serde_json::Value> = values
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<_, _>>()?;
    Ok(serde_json::to_string(&json)?)
}

/// `(pattern, return)` pairs; the source variable is always `x`.
const QUERIES: &[(&str, &str)] = &[
    (
        "(x)-[r:R]->(y)",
        "id(x), id(r), id(y), r.w, labels(x), labels(y), type(r)",
    ),
    (
        "(x)<-[r:R]-(y)",
        "id(x), id(r), id(y), r.w, labels(x), labels(y)",
    ),
    ("(x)-[r:R]-(y)", "id(x), id(r), id(y), r.w"),
    ("(x)-[r:R|S]->(y)", "id(x), id(r), id(y), r.w, type(r)"),
    ("(x)-[r:R|S]-(y)", "id(x), id(r), id(y), type(r)"),
    ("(x)-[r:R]->(y)", "x, r, y"),
    ("(x)-[r:R]-(y)", "x.v, r, y.v"),
    ("(x)-[r:R]->(y)", "count(r), max(r.w), min(r.w)"),
    ("(x)-[r:R]-()", "count(*)"),
    ("(x)-[r:R]->(x)", "id(x), id(r)"),
    ("(x)-[:R]->(y)", "x.v, y.v"),
    ("(x)-[r]->(y)", "id(x), id(r), id(y), type(r)"),
    ("(x)-[r]-(y)", "id(x), id(r), id(y)"),
    (
        "(x)-[r:R]->()",
        "id(r), id(startNode(r)), id(endNode(r)), r.w",
    ),
];

/// How the edge-driven arm is expected to read its source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    /// `EdgeSourceScanExec` reads only edge sources: far fewer rows than the
    /// [`ISOLATED`] edge-less vertices alone.
    Restricted,
    /// `EdgeSourceScanExec` reads every vertex (an SSI read set).
    Unrestricted,
}

/// Every query in [`QUERIES`] returns the same rows on both paths, and the
/// edge-driven arm reads its source as `source` says. Returns the directed
/// `R` edge count.
async fn assert_equivalent(reader: Reader<'_>, phase: &str, source: Source) -> Result<i64> {
    for (pattern, ret) in QUERIES {
        let fast = format!("MATCH {pattern} RETURN {ret}");
        let forced = format!("MATCH {pattern} WHERE id(x) >= 0 RETURN {ret}");
        let (fast_rows, fast_ops, read) = reader.run_counted(&fast).await?;
        let (forced_rows, forced_ops) = reader.run(&forced).await?;
        assert!(
            !forced_ops.iter().any(|op| op == EDGE_SOURCE),
            "{phase}: the forced arm must keep the all-vertex scan: {forced}\n{forced_ops:?}"
        );
        assert!(
            forced_ops.iter().any(|op| op == "GraphTraverseExec"),
            "{phase}: {forced}\n{forced_ops:?}"
        );
        let read = read.with_context(|| {
            format!("{phase}: expected the edge-driven source: {fast}\n{fast_ops:?}")
        })?;
        match source {
            Source::Restricted => assert!(
                read < ISOLATED,
                "{phase}: the source scan was not restricted ({read} rows): {fast}"
            ),
            Source::Unrestricted => assert!(
                read > ISOLATED,
                "{phase}: the source scan must read every vertex ({read} rows): {fast}"
            ),
        }
        assert_eq!(fast_rows, forced_rows, "{phase}: {fast}");
    }
    let (rows, _) = reader.run("MATCH ()-[r:R]->() RETURN count(r)").await?;
    let count: i64 = rows[0]
        .trim_matches(|c| c == '[' || c == ']')
        .parse()
        .context("count")?;
    Ok(count)
}

/// `R: A|B -> B|C` (two source labels, so never label-narrowed) and
/// `S: A -> C`, both carrying `w`, plus vertices with no edges at all.
async fn declare(db: &Uni) -> Result<()> {
    db.schema()
        .label("A")
        .property("v", DataType::Int)
        .label("B")
        .property("v", DataType::Int)
        .label("C")
        .property("v", DataType::Int)
        .edge_type("R", &["A", "B"], &["B", "C"])
        .property("w", DataType::Int)
        .edge_type("S", &["A"], &["C"])
        .property("w", DataType::Int)
        .apply()
        .await?;
    Ok(())
}

/// Edge-less vertices in every fixture. They make the candidates a small
/// fraction of the vertex table, which is when the source scan is restricted
/// rather than read whole (`vertex_scan_beats_lookup`), and they are what a
/// restriction must not lose rows over.
const ISOLATED: usize = 200;

/// Five `R` edges (one a self-loop on `b2`) and two `S` edges; `c3` and the
/// `A`/`B` vertices numbered 9 have no edges, nor do [`ISOLATED`] others.
async fn seed(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(1, {ISOLATED}) AS i CREATE (:A {{v: 1000 + i}})"
    ))
    .await?;
    tx.execute(
        "CREATE (a1:A {v: 1}), (a2:A {v: 2}), (:A {v: 9}), \
                (b1:B {v: 11}), (b2:B {v: 12}), (:B {v: 19}), \
                (c1:C {v: 21}), (c2:C {v: 22}), (:C {v: 23}), \
                (a1)-[:R {w: 1}]->(b1), (a1)-[:R {w: 2}]->(c1), \
                (a2)-[:R {w: 3}]->(c1), (b1)-[:R {w: 4}]->(c2), \
                (b2)-[:R {w: 5}]->(b2), \
                (a1)-[:S {w: 6}]->(c2), (a2)-[:S {w: 7}]->(c2)",
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn exec(db: &Uni, cypher: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher)
        .await
        .with_context(|| cypher.to_string())?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn edge_source_scan_l0_only() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    declare(&db).await?;
    seed(&db).await?;
    let s = db.session();
    assert_eq!(
        assert_equivalent(Reader::Session(&s), "L0", Source::Restricted).await?,
        5
    );
    Ok(())
}

#[tokio::test]
async fn edge_source_scan_flushed_mixed_and_deleted() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().context("utf-8")?)
        .build()
        .await?;
    declare(&db).await?;
    seed(&db).await?;
    db.flush().await?;
    let s = db.session();
    assert_eq!(
        assert_equivalent(Reader::Session(&s), "flushed", Source::Restricted).await?,
        5
    );

    // Unflushed edges on top: a new source (`b9` had no edge), a second
    // self-loop, and a deleted flushed edge.
    exec(
        &db,
        "MATCH (b:B {v: 19}), (c:C {v: 23}) CREATE (b)-[:R {w: 8}]->(c), (c)<-[:R {w: 9}]-(b)",
    )
    .await?;
    exec(
        &db,
        "MATCH (c:C {v: 22}) CREATE (:A {v: 30})-[:R {w: 10}]->(c)",
    )
    .await?;
    exec(&db, "MATCH ()-[r:R {w: 4}]->() DELETE r").await?;
    assert_eq!(
        assert_equivalent(Reader::Session(&s), "mixed", Source::Restricted).await?,
        7
    );

    // Delete an L0 edge and a flushed source with all its edges.
    exec(&db, "MATCH ()-[r:R {w: 9}]->() DELETE r").await?;
    exec(&db, "MATCH (a:A {v: 1}) DETACH DELETE a").await?;
    assert_eq!(
        assert_equivalent(Reader::Session(&s), "deleted", Source::Restricted).await?,
        4
    );
    db.flush().await?;
    assert_eq!(
        assert_equivalent(Reader::Session(&s), "deleted, flushed", Source::Restricted).await?,
        4
    );
    db.shutdown().await?;
    let db = Uni::open(dir.path().to_str().context("utf-8")?)
        .build()
        .await?;
    let s = db.session();
    assert_eq!(
        assert_equivalent(Reader::Session(&s), "reopened", Source::Restricted).await?,
        4
    );
    Ok(())
}

#[tokio::test]
async fn edge_source_scan_fork() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = UniConfig {
        disable_fork_sweeper: true,
        ..Default::default()
    };
    let db = Uni::open(dir.path().to_str().context("utf-8")?)
        .config(config)
        .build()
        .await?;
    declare(&db).await?;
    seed(&db).await?;
    db.flush().await?;
    let fork = db.session().fork("f").await?;
    let tx = fork.tx().await?;
    tx.execute("MATCH (b:B {v: 19}), (c:C {v: 23}) CREATE (b)-[:R {w: 40}]->(c)")
        .await?;
    tx.execute("MATCH ()-[r:R {w: 1}]->() DELETE r").await?;
    tx.commit().await?;
    assert_eq!(
        assert_equivalent(
            Reader::Session(&fork),
            "fork, unflushed",
            Source::Restricted
        )
        .await?,
        5
    );
    fork.flush().await?;
    assert_eq!(
        assert_equivalent(Reader::Session(&fork), "fork, flushed", Source::Restricted).await?,
        5
    );
    let primary = db.session();
    assert_eq!(
        assert_equivalent(
            Reader::Session(&primary),
            "primary beside the fork",
            Source::Restricted
        )
        .await?,
        5
    );
    let (rows, _) = Reader::Session(&primary)
        .run("MATCH ()-[r:R]->() RETURN r.w ORDER BY r.w")
        .await?;
    assert_eq!(
        rows,
        ["[1]", "[2]", "[3]", "[4]", "[5]"],
        "primary unchanged"
    );
    Ok(())
}

#[tokio::test]
async fn edge_source_scan_bulk_load_in_flight() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().context("utf-8")?)
        .build()
        .await?;
    declare(&db).await?;
    seed(&db).await?;
    db.flush().await?;
    let reader = db.session();
    let session = db.session();
    let tx = session.tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(2).build()?;
    let row = |v: i64| HashMap::from([("v".to_string(), Value::Int(v))]);
    let a = bulk
        .insert_vertices("A", vec![row(50), row(51), row(52)])
        .await?;
    let c = bulk.insert_vertices("C", vec![row(60)]).await?;
    let w = |w: i64| HashMap::from([("w".to_string(), Value::Int(w))]);
    bulk.insert_edges(
        "R",
        vec![
            EdgeData::new(a[0], c[0], w(70)),
            EdgeData::new(a[1], c[0], w(71)),
            EdgeData::new(a[2], c[0], w(72)),
        ],
    )
    .await?;
    assert_eq!(
        assert_equivalent(
            Reader::Session(&reader),
            "bulk in flight",
            Source::Restricted
        )
        .await?,
        5,
        "the uncommitted load is hidden on both paths"
    );
    bulk.commit().await?;
    tx.commit().await?;
    assert_eq!(
        assert_equivalent(
            Reader::Session(&reader),
            "bulk committed",
            Source::Restricted
        )
        .await?,
        8
    );
    Ok(())
}

/// Inside a transaction its own uncommitted edges are traversal sources. With
/// SSI on the scan is not restricted (the read set must record every vertex),
/// with it off the transaction L0 supplies candidates.
#[tokio::test]
async fn edge_source_scan_in_transaction() -> Result<()> {
    for ssi in [true, false] {
        let config = UniConfig {
            ssi_enabled: ssi,
            ..Default::default()
        };
        let db = Uni::in_memory().config(config).build().await?;
        declare(&db).await?;
        seed(&db).await?;
        db.flush().await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute(
            "MATCH (b:B {v: 19}), (c:C {v: 23}) CREATE (b)-[:R {w: 80}]->(c), (b)-[:R {w: 81}]->(b)",
        )
        .await?;
        tx.execute("MATCH ()-[r:R {w: 2}]->() DELETE r").await?;
        let phase = format!("in transaction, ssi={ssi}");
        let source = if ssi {
            Source::Unrestricted
        } else {
            Source::Restricted
        };
        assert_eq!(assert_equivalent(Reader::Tx(&tx), &phase, source).await?, 6);
        tx.rollback();
        assert_eq!(
            assert_equivalent(Reader::Session(&session), "rolled back", Source::Restricted).await?,
            5
        );
    }
    Ok(())
}

/// A pinned snapshot reads the edges it held, through the shadow CSR for
/// those deleted since.
#[tokio::test]
async fn edge_source_scan_pinned_snapshot() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().context("utf-8")?)
        .build()
        .await?;
    declare(&db).await?;
    seed(&db).await?;
    db.flush().await?;
    let snapshot = db.create_snapshot("before").await?;
    exec(&db, "MATCH ()-[r:R {w: 3}]->() DELETE r").await?;
    exec(
        &db,
        "MATCH (b:B {v: 19}), (c:C {v: 23}) CREATE (b)-[:R {w: 90}]->(c)",
    )
    .await?;
    db.flush().await?;
    let live = db.session();
    assert_eq!(
        assert_equivalent(Reader::Session(&live), "live", Source::Restricted).await?,
        5
    );
    let mut pinned = db.session();
    pinned.pin_to_version(&snapshot).await?;
    assert_eq!(
        assert_equivalent(Reader::Session(&pinned), "pinned", Source::Restricted).await?,
        5
    );
    let (rows, _) = Reader::Session(&pinned)
        .run("MATCH ()-[r:R]->() RETURN r.w ORDER BY r.w")
        .await?;
    assert_eq!(
        rows,
        ["[1]", "[2]", "[3]", "[4]", "[5]"],
        "the snapshot's edges"
    );
    Ok(())
}

/// A legacy (unverified) catalog and a verified one: both answer the same on
/// both paths. A verified type with one declared source label is narrowed by
/// the logical planner instead, and still agrees.
#[tokio::test]
async fn edge_source_scan_unverified_and_verified_types() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?.to_string();
    {
        let db = Uni::open(path.clone()).build().await?;
        declare(&db).await?;
        seed(&db).await?;
        db.shutdown().await?;
    }
    narrow_declaration(dir.path(), "R", &["A", "B"], &["B", "C"])?;
    narrow_declaration(dir.path(), "S", &["A"], &["C"])?;
    let db = Uni::open(path).build().await?;
    let s = db.session();
    assert_eq!(
        assert_equivalent(Reader::Session(&s), "unverified", Source::Restricted).await?,
        5
    );
    s.run("CALL uni.admin.verifyEdgeEndpoints() YIELD conformance RETURN conformance")
        .await?;
    let s = db.session();
    assert_eq!(
        assert_equivalent(Reader::Session(&s), "verified", Source::Restricted).await?,
        5
    );
    // `S` has one declared source label, so a verified `S` narrows to `A`.
    let narrowed = "MATCH (x)-[r:S]->(y) RETURN id(x), id(r), id(y)";
    assert_plan_avoids(&s, narrowed, EDGE_SOURCE).await;
    let (fast, _) = Reader::Session(&s).run(narrowed).await?;
    let (forced, _) = Reader::Session(&s)
        .run("MATCH (x)-[r:S]->(y) WHERE id(x) >= 0 RETURN id(x), id(r), id(y)")
        .await?;
    assert_eq!(fast.len(), 2);
    assert_eq!(fast, forced);
    Ok(())
}

/// Where the edge-driven source is planned, and where it must not be.
#[tokio::test]
async fn edge_source_scan_plan_shape() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    declare(&db).await?;
    seed(&db).await?;
    let s = db.session();
    for uses in [
        "MATCH ()-[r:R]->() RETURN count(r)",
        "MATCH ()<-[r:R]-() RETURN count(r)",
        "MATCH ()-[r:R]-() RETURN count(r)",
        "MATCH (x)-[r:R|S]->(y) RETURN x, r, y",
        // Every type: still a fixed set of native types.
        "MATCH ()-[r]->() RETURN count(r)",
    ] {
        assert_plan_uses(&s, uses, "EdgeSourceScanExec").await;
    }
    for avoids in [
        // A labelled source scans its label.
        "MATCH (x:A)-[r:R]->() RETURN count(r)",
        // A filtered source.
        "MATCH (x {v: 1})-[r:R]->() RETURN count(r)",
        // An optional hop keeps sources with no edge.
        "OPTIONAL MATCH (x)-[r:R]->() RETURN count(x)",
        // Variable length.
        "MATCH ()-[r:R*1..2]->() RETURN count(r)",
    ] {
        assert_plan_avoids(&s, avoids, EDGE_SOURCE).await;
    }
    Ok(())
}

/// In a read-write transaction with SSI the source scan is not restricted, so
/// the transaction's read set — and with it which concurrent commits fail it —
/// is the all-vertex plan's: every vertex, including those with no `R` edge.
/// Each concurrent write below touches only such a vertex.
#[tokio::test]
async fn edge_source_scan_transaction_read_set_matches_traverse() -> Result<()> {
    let concurrent = [
        "MATCH (b:B {v: 19}), (c:C {v: 23}) CREATE (b)-[:R {w: 100}]->(c)",
        "MATCH (c:C {v: 23}) SET c.v = 24",
        "MATCH (a:A {v: 9}) DELETE a",
    ];
    for write in concurrent {
        let mut outcomes = Vec::new();
        for query in [
            "MATCH (x)-[r:R]->() RETURN count(r)",
            "MATCH (x)-[r:R]->() WHERE id(x) >= 0 RETURN count(r)",
        ] {
            let db = Uni::in_memory().build().await?;
            declare(&db).await?;
            seed(&db).await?;
            db.flush().await?;
            let reader_session = db.session();
            let reader = reader_session.tx().await?;
            let r = reader.query(query).await?;
            assert_eq!(r.rows()[0].values()[0], Value::Int(5), "{query}");
            reader.execute("CREATE (:C {v: 99})").await?;
            exec(&db, write).await?;
            outcomes.push(reader.commit().await.is_ok());
        }
        assert_eq!(
            outcomes[0], outcomes[1],
            "{write}: commit outcome differs between the two source plans"
        );
    }
    Ok(())
}
