// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! MERGE must anchor on what the input row binds, not on what was written
//! first (#225).
//!
//! `execute_merge_match` carries its own copy of `plan_path`'s left-to-right
//! element walk, so neither #219's anchoring fix nor `dc232cd2a`'s ranking of
//! it reached MERGE. Boundness was consulted only for the leftmost node, which
//! made `MERGE (a:L)-[:R]->(b)` with `b` bound scan the whole of `L` — once per
//! input row, since the walk runs per row.
//!
//! # Why this compares two spellings rather than a counter or a growth curve
//!
//! **Not `rows_scanned`.** It cannot see this. MERGE's per-row plans never
//! reach the counter, so every spelling reports only what the *outer* MATCH
//! examined: the probe measured 395 for a sixteen-second arm and 395 for a
//! sub-second one. A test asserting on it would pass in both directions.
//!
//! **Not a growth curve either**, though that is what settled the diagnosis. A
//! per-row scan of a label costs in proportion to that label, so holding the
//! batch fixed and growing the label separates a scan from an anchored lookup —
//! `examples/merge_anchor_probe` does exactly that, and shows 5.4s → 16.2s
//! before and 0.70s → 0.76s after. It does not survive being shrunk into a
//! test: over an `Uni::in_memory()` fixture the defect measures **0.83x** for a
//! 4x larger label, the larger graph coming out *faster*. The probe's fixture
//! is on disk and flushed, and the L0-resident one does not scale the same way.
//! A growth assertion here would have been a test that cannot fail.
//!
//! **So: the same link, written two ways, over one fixture in one process.**
//! Machine speed divides out of the ratio, and the two spellings are asserted
//! to create the same relationships before any time is compared. Verified to
//! discriminate: 4.12x with the anchor reverted, 1.10x with it in place.
//!
//! **Parallelism:** the numbers are wall-clock, but both sides run in the same
//! process on the same data, so load slows them together. A failure close to
//! the guard should be re-run with `cargo nextest run -j1` before being read as
//! noise.

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{Uni, Value};

/// A 4x span, at sizes the effect actually clears.
///
/// An earlier version used 6 000 nodes and a 100-row batch, and **passed with
/// the fix reverted**: the per-row scan at that size is smaller than MERGE's
/// fixed per-row overhead and disappears into it. At 20 000 the ratio is 4.12x
/// reverted against 1.10x fixed. A guard is only worth having at a size where
/// the thing it guards is visible.
const LARGE: usize = 20_000;
const BATCH: usize = 200;

async fn graph(nodes: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE OWNS FROM Entity TO Entity")
        .await?;
    tx.execute("CREATE INDEX idx_uid FOR (e:Entity) ON (e.uid)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..nodes)
        .map(|i| HashMap::from([("uid".to_string(), Value::String(format!("e{i}")))]))
        .collect();
    bulk.insert_vertices("Entity", vertices).await?;
    bulk.commit().await?;
    tx.commit().await?;
    Ok(db)
}

/// `BATCH` deterministic src/dst pairs drawn from `nodes`.
fn batch(nodes: usize) -> Value {
    let mut state = 11u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 11) % nodes as u64) as usize
    };
    Value::List(
        (0..BATCH)
            .map(|_| {
                Value::Map(HashMap::from([
                    ("src".to_string(), Value::String(format!("e{}", next()))),
                    ("dst".to_string(), Value::String(format!("e{}", next()))),
                ]))
            })
            .collect(),
    )
}

/// Returns (seconds, relationships created), against an existing graph.
///
/// The transaction is rolled back rather than committed so a later run cannot
/// see edges an earlier one created — which would turn a MERGE insert into a
/// MERGE match and measure something else.
async fn run_on(db: &Uni, nodes: usize, query: &str) -> Result<(f64, usize)> {
    let tx = db.session().tx().await?;
    let started = std::time::Instant::now();
    let res = tx
        .execute_with(query)
        .param("batch", batch(nodes))
        .run()
        .await?;
    let secs = started.elapsed().as_secs_f64();
    let created = res.relationships_created();
    tx.rollback();
    Ok((secs, created))
}

/// `b` is bound by the preceding MATCH but written at the *tail* of the MERGE
/// pattern, so the walk's first element is the unbound `a`.
const BOUND_LAST: &str = "UNWIND $batch AS r \
                          MATCH (b:Entity {uid: r.dst}) \
                          MERGE (a:Entity {uid: r.src})-[e:OWNS]->(b)";

/// The same link, written so the bound node comes first.
const BOUND_FIRST: &str = "UNWIND $batch AS r \
                           MATCH (a:Entity {uid: r.src}) \
                           MERGE (a)-[e:OWNS]->(b:Entity {uid: r.dst})";

/// The two spellings describe the same link and should cost about the same.
///
/// Separate from the growth test because the two catch different regressions: a
/// change that made *both* spellings scan would keep the ratio here at 1 while
/// the growth test fails, and one that slowed only the reversal path would do
/// the reverse.
#[tokio::test]
async fn issue_225_both_spellings_of_the_same_link_cost_alike() -> Result<()> {
    let db = graph(LARGE).await?;
    let (last, last_created) = run_on(&db, LARGE, BOUND_LAST).await?;
    let (first, first_created) = run_on(&db, LARGE, BOUND_FIRST).await?;

    assert_eq!(
        last_created, first_created,
        "the two spellings created {last_created} and {first_created} \
         relationships; they are not describing the same link"
    );
    assert_eq!(last_created, BATCH, "fixture linked {last_created} pairs");

    let ratio = last / first.max(1e-9);
    eprintln!("bound-last {last:.3}s vs bound-first {first:.3}s = {ratio:.2}x");
    assert!(
        ratio < 1.8,
        "writing the bound node last cost {ratio:.2}x writing it first \
         ({last:.3}s vs {first:.3}s) for the same link — MERGE is not anchoring \
         on the binding (#225)"
    );
    Ok(())
}

/// Naming the relationship must not drop MERGE onto the per-row plan.
///
/// `merge_relationship_fastpath_shape` rejected any pattern whose relationship
/// carried a variable, so `MERGE (a)-[e:R]->(b)` — the canonical batched
/// edge-ingest shape — paid a rebuilt `LogicalPlan` and a full DataFusion
/// planning pass per input row, while `MERGE (a)-[:R]->(b)` ran at the
/// MATCH+CREATE floor. Measured at 20 000 vertices with a 400-row batch:
/// 0.79s named against 0.11s anonymous, and 0.105s named after.
///
/// The ratio is against the anonymous spelling rather than an absolute
/// threshold, so the machine divides out: the two differ in one character and
/// do the same work.
#[tokio::test]
async fn issue_225_naming_the_relationship_keeps_the_fast_path() -> Result<()> {
    const NAMED: &str = "UNWIND $batch AS r \
                         MATCH (a:Entity {uid: r.src}), (b:Entity {uid: r.dst}) \
                         MERGE (a)-[e:OWNS]->(b)";
    const ANON: &str = "UNWIND $batch AS r \
                        MATCH (a:Entity {uid: r.src}), (b:Entity {uid: r.dst}) \
                        MERGE (a)-[:OWNS]->(b)";

    let db = graph(LARGE).await?;
    let (named, named_created) = run_on(&db, LARGE, NAMED).await?;
    let (anon, anon_created) = run_on(&db, LARGE, ANON).await?;

    assert_eq!(
        named_created, anon_created,
        "the two spellings created {named_created} and {anon_created} \
         relationships; they are not doing the same work"
    );
    assert_eq!(named_created, BATCH, "fixture linked {named_created} pairs");

    let ratio = named / anon.max(1e-9);
    eprintln!("named {named:.3}s vs anonymous {anon:.3}s = {ratio:.2}x");
    assert!(
        ratio < 3.0,
        "naming the relationship cost {ratio:.2}x leaving it anonymous \
         ({named:.3}s vs {anon:.3}s). A relationship variable is disqualifying \
         the MERGE fast path, so every row rebuilds and re-plans (#225)."
    );
    Ok(())
}

/// The fast path must not cost the named relationship its binding.
///
/// This is the reason the *match* outcome still falls back: an `Edge` value
/// carries properties, and they cannot be enumerated for an arbitrary edge. The
/// created outcome is served by the fast path and must bind exactly what the
/// general path would.
#[tokio::test]
async fn issue_225_a_named_relationship_is_still_bound() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE OWNS (pct FLOAT) FROM Entity TO Entity")
        .await?;
    tx.execute("CREATE (:Entity {uid: 'a'}), (:Entity {uid: 'b'})")
        .await?;
    tx.commit().await?;

    // Created through the fast path, with ON CREATE SET writing through the
    // binding: if `e` were unbound the SET would have nothing to write to.
    let tx = db.session().tx().await?;
    let created = tx
        .query(
            "MATCH (a:Entity {uid: 'a'}), (b:Entity {uid: 'b'}) \
             MERGE (a)-[e:OWNS]->(b) ON CREATE SET e.pct = 42.0 \
             RETURN type(e) AS t, e.pct AS p",
        )
        .await?;
    tx.commit().await?;
    assert_eq!(created.rows().len(), 1, "MERGE returned no row");
    assert_eq!(created.rows()[0].get::<String>("t")?, "OWNS");
    assert_eq!(created.rows()[0].get::<f64>("p")?, 42.0);

    // Matched this time. The fast path declines the match outcome for a named
    // relationship, so this exercises the fallback — and must agree.
    let tx = db.session().tx().await?;
    let matched = tx
        .query(
            "MATCH (a:Entity {uid: 'a'}), (b:Entity {uid: 'b'}) \
             MERGE (a)-[e:OWNS]->(b) \
             RETURN type(e) AS t, e.pct AS p",
        )
        .await?;
    tx.commit().await?;
    assert_eq!(matched.rows().len(), 1, "MERGE returned no row on match");
    assert_eq!(
        matched.rows()[0].get::<String>("t")?,
        "OWNS",
        "a matched MERGE lost the relationship's type"
    );
    assert_eq!(
        matched.rows()[0].get::<f64>("p")?,
        42.0,
        "a matched MERGE lost the relationship's properties — the fast path \
         served an outcome it cannot bind faithfully"
    );

    // And exactly one edge exists: MERGE matched rather than creating a second.
    let n = db
        .session()
        .query("MATCH (:Entity)-[e:OWNS]->(:Entity) RETURN count(e) AS n")
        .await?;
    assert_eq!(n.rows()[0].get::<i64>("n")?, 1, "MERGE created a duplicate");
    Ok(())
}

/// A keyed far endpoint must not drop MERGE onto the per-row plan.
///
/// `MERGE (a)-[e:R]->(b:L {k: v})` with `a` bound and `b` found-or-created was
/// served by neither fast path — `merge_single_node_fastpath` wants a lone node
/// and `merge_relationship_fastpath_shape` rejects an endpoint with properties —
/// so every row rebuilt a `LogicalPlan` and ran a full DataFusion planning pass.
/// Measured at 20 000 vertices with a 400-row batch: 0.81s before, 0.065s after,
/// against a 0.11s MATCH+CREATE floor.
///
/// Compared against that floor rather than an absolute threshold, so the machine
/// divides out. The fast path can legitimately come in *under* the floor — it
/// resolves one endpoint where the floor's two-MATCH form resolves two — so the
/// guard is one-sided.
#[tokio::test]
async fn issue_225_a_keyed_far_endpoint_keeps_a_fast_path() -> Result<()> {
    const KEYED: &str = "UNWIND $batch AS r \
                         MATCH (a:Entity {uid: r.src}) \
                         MERGE (a)-[e:OWNS]->(b:Entity {uid: r.dst})";
    // The same writes with no MERGE at all.
    const FLOOR: &str = "UNWIND $batch AS r \
                         MATCH (a:Entity {uid: r.src}), (b:Entity {uid: r.dst}) \
                         CREATE (a)-[e:OWNS]->(b)";

    let db = graph(LARGE).await?;
    let (keyed, keyed_created) = run_on(&db, LARGE, KEYED).await?;
    let (floor, floor_created) = run_on(&db, LARGE, FLOOR).await?;

    assert_eq!(
        keyed_created, BATCH,
        "the keyed arm linked {keyed_created} of {BATCH} pairs"
    );
    assert_eq!(
        floor_created, BATCH,
        "the floor arm linked {floor_created} of {BATCH} pairs"
    );

    let ratio = keyed / floor.max(1e-9);
    eprintln!("keyed endpoint {keyed:.3}s vs MATCH+CREATE floor {floor:.3}s = {ratio:.2}x");
    assert!(
        ratio < 3.0,
        "a keyed far endpoint cost {ratio:.2}x the MATCH+CREATE floor \
         ({keyed:.3}s vs {floor:.3}s), so MERGE is running its per-row plan for \
         this shape instead of resolving the endpoint from the batch's key map \
         (#225)."
    );
    Ok(())
}

/// A named relationship stays on the fast path when the edge already exists.
///
/// Creating a named edge always took the fast path — `execute_create_pattern`
/// binds it with the properties it just wrote. *Matching* one did not: binding
/// it needs the existing edge's value, which the fast path handed back to the
/// general per-row plan. That made cost depend on whether the data was already
/// there, so a re-ingest paid the per-row plan on every row while the first
/// ingest paid none.
///
/// It is fixed by reading the edge the adjacency probe found:
/// `get_all_edge_props_with_ctx` enumerates one edge's properties, which is
/// what the earlier "they cannot be enumerated" reasoning got wrong — that is
/// true of the *batch* reader, not of the single-edge one.
///
/// The second run of the same batch is 100% matches, so it is the arm that
/// would regress. Compared against the first run rather than a threshold.
#[tokio::test]
async fn issue_225_a_matched_named_relationship_stays_on_the_fast_path() -> Result<()> {
    const NAMED: &str = "UNWIND $batch AS r \
                         MATCH (a:Entity {uid: r.src}), (b:Entity {uid: r.dst}) \
                         MERGE (a)-[e:OWNS]->(b)";

    let db = graph(LARGE).await?;

    // Commit the first run so the second sees the edges as existing.
    let tx = db.session().tx().await?;
    let started = std::time::Instant::now();
    let first = tx
        .execute_with(NAMED)
        .param("batch", batch(LARGE))
        .run()
        .await?;
    let create_secs = started.elapsed().as_secs_f64();
    let created = first.relationships_created();
    tx.commit().await?;

    // Second run: every row matches.
    let tx = db.session().tx().await?;
    let started = std::time::Instant::now();
    let second = tx
        .execute_with(NAMED)
        .param("batch", batch(LARGE))
        .run()
        .await?;
    let match_secs = started.elapsed().as_secs_f64();
    let created_again = second.relationships_created();
    tx.rollback();

    assert_eq!(created, BATCH, "first run linked {created} of {BATCH}");
    assert_eq!(
        created_again, 0,
        "the second run created {created_again} relationships; it should have \
         matched every row"
    );

    let ratio = match_secs / create_secs.max(1e-9);
    eprintln!("named edge: create {create_secs:.3}s, match {match_secs:.3}s = {ratio:.2}x");
    assert!(
        ratio < 3.0,
        "matching an existing named relationship cost {ratio:.2}x creating one \
         ({match_secs:.3}s vs {create_secs:.3}s), so the match outcome is \
         falling back to the per-row plan (#225)."
    );
    Ok(())
}
