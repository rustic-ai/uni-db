// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Quantified path patterns with per-hop property maps, against fixed-length
//! references.
//!
//! `(a) ((x)-[:R {w: 1}]->(y:N {x: 1})-[:S]->(z)){1,2} (b)` gates each hop of
//! each iteration on its own maps. The reference spells the one- and
//! two-iteration paths out as ordinary fixed-length patterns with the maps
//! written in `WHERE`, so it reaches the answer without the quantified walk
//! or its per-hop filters; relationship uniqueness holds within a `MATCH` in
//! both. Rows are compared as bags of `(a.id, b.id)`.
//!
//! The maps are read over the region the walk can reach rather than over the
//! whole edge type, so the graphs mix flushed and unflushed edges. A
//! multi-hop quantified pattern needs declared relationship types, so every
//! graph declares its schema.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(qpp_region_map_oracle)'

// Rust guideline compliant

use anyhow::{Result, anyhow};
use uni_db::{DataType, Session, Uni, Value};

/// SplitMix64, so a failing seed reproduces exactly.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Clone, Debug)]
struct Edge {
    ty: &'static str,
    src: usize,
    dst: usize,
    w: Option<i64>,
}

#[derive(Clone, Debug)]
struct Graph {
    x: Vec<Option<i64>>,
    edges: Vec<Edge>,
}

fn random_graph(seed: u64) -> Graph {
    let mut rng = Rng(seed);
    let n = 6 + rng.below(5) as usize;
    let x = (0..n)
        .map(|_| match rng.below(4) {
            0 => None,
            v => Some((v % 2) as i64),
        })
        .collect();
    let m = 2 * n + rng.below(2 * n as u64) as usize;
    let edges = (0..m)
        .map(|_| Edge {
            ty: if rng.below(2) == 0 { "R" } else { "S" },
            src: rng.below(n as u64) as usize,
            dst: rng.below(n as u64) as usize,
            w: match rng.below(4) {
                0 => None,
                v => Some((v % 2) as i64),
            },
        })
        .collect();
    Graph { x, edges }
}

async fn declare_schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property_nullable("x", DataType::Int64)
        .edge_type("R", &["N"], &["N"])
        .property_nullable("w", DataType::Int64)
        .edge_type("S", &["N"], &["N"])
        .property_nullable("w", DataType::Int64)
        .apply()
        .await?;
    Ok(())
}

async fn load_edges(session: &Session, edges: &[Edge]) -> Result<()> {
    let tx = session.tx().await?;
    for ty in ["R", "S"] {
        let rows: Vec<String> = edges
            .iter()
            .filter(|e| e.ty == ty)
            .map(|e| {
                let w = e.w.map_or("null".to_string(), |w| w.to_string());
                format!("[{}, {}, {w}]", e.src, e.dst)
            })
            .collect();
        if rows.is_empty() {
            continue;
        }
        tx.execute(&format!(
            "UNWIND [{}] AS e MATCH (a:N {{id: e[0]}}), (b:N {{id: e[1]}}) \
             CREATE (a)-[:{ty} {{w: e[2]}}]->(b)",
            rows.join(", ")
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn bag(session: &Session, q: &str) -> Result<Vec<String>> {
    let res = session.query(q).await.map_err(|e| anyhow!("{q}: {e}"))?;
    let mut rows: Vec<String> = res
        .rows()
        .iter()
        .map(|r| {
            r.values()
                .iter()
                .map(|v| match v {
                    Value::Int(i) => i.to_string(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    rows.sort();
    Ok(rows)
}

/// A quantified pattern and its reference: the one-iteration and
/// two-iteration fixed-length forms, summed as a bag.
struct Case {
    qpp: &'static str,
    one: &'static str,
    two: &'static str,
}

const CASES: &[Case] = &[
    // An edge map on the first hop, none on the second.
    Case {
        qpp: "MATCH (a:N) MATCH (a)((x)-[:R {w: 1}]->(y)-[:S]->(z)){1,2}(b) RETURN a.id, b.id",
        one: "MATCH (a:N)-[r1:R]->()-[:S]->(b) WHERE r1.w = 1 RETURN a.id, b.id",
        two: "MATCH (a:N)-[r1:R]->()-[:S]->()-[r2:R]->()-[:S]->(b) \
              WHERE r1.w = 1 AND r2.w = 1 RETURN a.id, b.id",
    },
    // Maps on both hops and on the inner vertex, mixed directions.
    Case {
        qpp: "MATCH (a:N) MATCH (a)((x)-[:R {w: 1}]->(y:N {x: 1})<-[:S {w: 0}]-(z)){1,2}(b) \
              RETURN a.id, b.id",
        one: "MATCH (a:N)-[r1:R]->(y1:N)<-[s1:S]-(b) \
              WHERE r1.w = 1 AND y1.x = 1 AND s1.w = 0 RETURN a.id, b.id",
        two: "MATCH (a:N)-[r1:R]->(y1:N)<-[s1:S]-()-[r2:R]->(y2:N)<-[s2:S]-(b) \
              WHERE r1.w = 1 AND y1.x = 1 AND s1.w = 0 \
                AND r2.w = 1 AND y2.x = 1 AND s2.w = 0 RETURN a.id, b.id",
    },
    // A vertex map on the iteration's last node, which is also the endpoint
    // of every iteration, an undirected hop, and an anchored source.
    Case {
        qpp: "MATCH (a:N {id: 0}) MATCH (a)((x)-[:S]-(y)-[:R {w: 0}]->(z:N {x: 0})){1,2}(b) \
              RETURN a.id, b.id",
        one: "MATCH (a:N {id: 0})-[:S]-()-[r1:R]->(b:N) WHERE r1.w = 0 AND b.x = 0 \
              RETURN a.id, b.id",
        two: "MATCH (a:N {id: 0})-[:S]-()-[r1:R]->(z1:N)-[:S]-()-[r2:R]->(b:N) \
              WHERE r1.w = 0 AND z1.x = 0 AND r2.w = 0 AND b.x = 0 RETURN a.id, b.id",
    },
    // The same type on both hops, with different maps.
    Case {
        qpp: "MATCH (a:N) MATCH (a)((x)-[:R {w: 1}]->(y)-[:R {w: 0}]->(z)){1,2}(b) \
              RETURN a.id, b.id",
        one: "MATCH (a:N)-[r1:R]->()-[r2:R]->(b) WHERE r1.w = 1 AND r2.w = 0 RETURN a.id, b.id",
        two: "MATCH (a:N)-[r1:R]->()-[r2:R]->()-[r3:R]->()-[r4:R]->(b) \
              WHERE r1.w = 1 AND r2.w = 0 AND r3.w = 1 AND r4.w = 0 RETURN a.id, b.id",
    },
];

async fn check(session: &Session, ctx: &str) -> Result<()> {
    let mut failures = Vec::new();
    for c in CASES {
        let mut want = bag(session, c.one).await?;
        want.extend(bag(session, c.two).await?);
        want.sort();
        match bag(session, c.qpp).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{}\n  got:  {got:?}\n  want: {want:?}", c.qpp)),
            Err(e) => failures.push(format!("failed: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{ctx}\n{} of {} cases differ:\n{}",
        failures.len(),
        CASES.len(),
        failures.join("\n")
    );
    Ok(())
}

async fn run(seed: u64) -> Result<()> {
    let g = random_graph(seed);
    let db = Uni::in_memory().build().await?;
    declare_schema(&db).await?;
    let session = db.session();
    let nodes: Vec<String> =
        g.x.iter()
            .enumerate()
            .map(|(i, x)| format!("[{i}, {}]", x.map_or("null".into(), |x| x.to_string())))
            .collect();
    let tx = session.tx().await?;
    tx.execute(&format!(
        "UNWIND [{}] AS r CREATE (:N {{id: r[0], x: r[1]}})",
        nodes.join(", ")
    ))
    .await?;
    tx.commit().await?;
    // Every other edge flushed, the rest in L0.
    let flushed: Vec<Edge> = g.edges.iter().step_by(2).cloned().collect();
    let l0: Vec<Edge> = g.edges.iter().skip(1).step_by(2).cloned().collect();
    load_edges(&session, &flushed).await?;
    db.flush().await?;
    load_edges(&session, &l0).await?;
    check(&session, &format!("seed {seed:#x}, mixed\n{g:?}")).await?;
    db.flush().await?;
    check(&session, &format!("seed {seed:#x}, all flushed\n{g:?}")).await?;
    db.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn qpp_per_hop_maps_match_the_reference() -> Result<()> {
    for seed in 0..8 {
        run(0x9B_0000 + seed).await?;
    }
    Ok(())
}
