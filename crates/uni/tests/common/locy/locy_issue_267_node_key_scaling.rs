// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Scaling guard for issue #267: `YIELD KEY <node>` vs `YIELD KEY <node>.<prop>`.
//!
//! #267 reported a non-recursive rule costing ~140x more with a node-valued KEY
//! column than with a scalar property of that same node, growing quadratically
//! where the scalar form grew linearly. Bisected to its parent `89c9b84f6`, the
//! reporter's own script measured 0.22s / 0.82s / 2.72s at 8k / 16k / 32k edges
//! against 0.05s / 0.10s / 0.23s for the scalar form — a 12x penalty and a 1.9
//! scaling exponent. `5a8dd387b` ("decode a UDF argument once per run of equal
//! rows", filed against #229/#245) took it to 2x and ~1.1.
//!
//! The fix was for a different issue and closed this one incidentally, which is
//! exactly the kind of thing that comes back: nothing in that commit's test set
//! mentions a node-valued KEY, so a future change to the UDF argument path has
//! no local signal that this shape depends on it. Hence this guard.
//!
//! # Why a ratio, and not a time
//!
//! The two arms differ in one word and run in the same process against the same
//! graph, so machine speed, allocator state and build profile divide out of
//! `node / scalar` in a way they do not divide out of a wall-clock threshold.
//! That is what makes a timing assertion tolerable here at all.
//!
//! It is still a timing assertion, so the guard sits at 4x: comfortably above
//! the ~2x the fix leaves and comfortably below the 12x the defect produced,
//! and both arms have to slow down *together* for a false pass. The equality
//! assertion below is what keeps that honest — an arm that got fast by
//! answering a smaller question fails before the ratio is ever consulted.
//!
//! **Parallelism note:** the measurement is CPU-bound and the ratio is taken
//! within one process, so co-scheduled tests affect both arms alike. It is not
//! pinned to a thread count and needs no `serial` treatment, but a regression
//! that lands it between 4x and 12x should be reproduced with
//! `cargo nextest run -j1` before being read as noise.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{Uni, Value};

/// Large enough for the defect to be unambiguous (8x at the parent commit,
/// against 2x after the fix) and small enough to stay a test rather than a
/// benchmark: both arms together run in about a second.
const N_EDGES: usize = 16_000;

const RULE_NODE_KEY: &str = "\
CREATE RULE r AS \
  MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
  WHERE o.blocked = true, e.blocked = false \
  FOLD agg = MSUM(s.pct) \
  WHERE agg >= 50.0 \
  YIELD KEY e, agg \n\
QUERY r RETURN agg";

const RULE_SCALAR_KEY: &str = "\
CREATE RULE r AS \
  MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
  WHERE o.blocked = true, e.blocked = false \
  FOLD agg = MSUM(s.pct) \
  WHERE agg >= 50.0 \
  YIELD KEY e.uid AS uid, agg \n\
QUERY r RETURN agg";

/// A deterministic LCG, so the graph is identical run to run and across
/// machines without taking a dependency for it.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
    fn unit(&mut self) -> f64 {
        (self.next_u64() % 1_000_000) as f64 / 1_000_000.0
    }
}

/// An ownership graph in the reporter's shape: half as many entities as edges,
/// ~13% of them blocked, every edge carrying a percentage. The rule's
/// `o.blocked = true, e.blocked = false` is what makes the derived set a
/// fraction of the edge set rather than all of it.
async fn build_graph() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (uid STRING, name STRING, blocked BOOL)")
        .await?;
    tx.execute("CREATE EDGE TYPE OWNS (pct FLOAT) FROM Entity TO Entity")
        .await?;
    tx.commit().await?;

    let n_nodes = N_EDGES / 2;
    let mut rng = Rng(7);
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..n_nodes)
        .map(|i| {
            let blocked = rng.unit() < 0.13;
            HashMap::from([
                ("uid".to_string(), Value::String(format!("e{i}"))),
                ("name".to_string(), Value::String(format!("Entity {i}"))),
                ("blocked".to_string(), Value::Bool(blocked)),
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("Entity", vertices).await?;

    let mut edges = Vec::with_capacity(N_EDGES);
    for _ in 0..N_EDGES {
        let (a, b) = (rng.below(n_nodes), rng.below(n_nodes));
        if a != b {
            let pct = 5.0 + rng.unit() * 95.0;
            edges.push(uni_bulk::EdgeData::new(
                vids[a],
                vids[b],
                HashMap::from([("pct".to_string(), Value::Float(pct))]),
            ));
        }
    }
    bulk.insert_edges("OWNS", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    Ok(db)
}

/// Returns (elapsed, derived fact count).
async fn measure(db: &Uni, program: &str) -> Result<(Duration, usize)> {
    let config = LocyConfig {
        max_iterations: 100,
        // Over-budget evaluation is a hard error rather than a silent partial,
        // so a timeout surfaces here instead of arriving as a fast, wrong count.
        timeout: Duration::from_secs(120),
        ..Default::default()
    };
    let started = Instant::now();
    let result = db
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await?;
    let elapsed = started.elapsed();
    let facts = result.derived_facts("r").map(|f| f.len()).unwrap_or(0);
    Ok((elapsed, facts))
}

#[tokio::test]
async fn issue_267_node_key_costs_about_what_a_scalar_key_costs() -> Result<()> {
    let db = build_graph().await?;

    // Scalar first, so the node arm cannot be credited with a warm-up the
    // scalar arm paid for. The reporter's script runs them the other way round
    // and the defect showed either way, but the order is free to get right.
    let (scalar, scalar_facts) = measure(&db, RULE_SCALAR_KEY).await?;
    let (node, node_facts) = measure(&db, RULE_NODE_KEY).await?;

    let ratio = node.as_secs_f64() / scalar.as_secs_f64().max(1e-9);
    eprintln!(
        "KEY e: {:.3}s / {node_facts} facts   KEY e.uid: {:.3}s / {scalar_facts} facts   ratio {ratio:.1}x",
        node.as_secs_f64(),
        scalar.as_secs_f64(),
    );

    // Correctness before cost: a cheap wrong answer is not a win. The two rules
    // group the same edges by the same entity under the same threshold, so they
    // must derive the same number of facts — differing only in whether the KEY
    // column holds the node or its `uid`.
    assert_eq!(
        node_facts, scalar_facts,
        "the node-KEY rule derived {node_facts} facts and the scalar-KEY rule \
         {scalar_facts}; the arms are not answering the same question, so the \
         timing comparison below would be meaningless"
    );
    assert!(
        node_facts > 100,
        "only {node_facts} facts derived — the fixture stopped exercising the \
         grouping path and the guard would pass on an empty rule"
    );

    assert!(
        ratio < 4.0,
        "a node-valued KEY cost {ratio:.1}x a scalar one ({:.3}s vs {:.3}s) at \
         {N_EDGES} edges, past the 4x guard. Issue #267: before `5a8dd387b` \
         this shape re-decoded a tagged CypherValue argument on every row, \
         which measured 12x here and grew quadratically. Check whether the \
         per-run argument memo in `invoke_cypher_udf` still covers it.",
        node.as_secs_f64(),
        scalar.as_secs_f64(),
    );
    Ok(())
}
