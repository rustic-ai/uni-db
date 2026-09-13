//! Does MERGE's private pattern walk anchor on a bound node? (#225)
//!
//! `execute_merge_match` carries its own transcription of `plan_path`'s
//! left-to-right element walk — its comment says so: "Reconstruct Match logic
//! from Planner (simplified for MERGE pattern)". It never calls `plan_path`,
//! `reversed_for_bound_anchor` or `split_at_bound_anchor`, so neither #219's
//! anchoring fix nor `dc232cd2a`'s ranking of it reaches MERGE. Boundness is
//! consulted only for the *leftmost* node; a bound node written later is
//! applied after execution as a row filter, and an unbound first node becomes a
//! `label_id = 0` ScanAll feeding a CrossJoin.
//!
//! The issue was filed from a source audit and says so plainly: "the impact
//! figure is inferred, not observed. Someone should write the repro before
//! estimating the fix." This is that repro.
//!
//! # Two costs are stacked here, and they need separating
//!
//! `execute_merge_match` is called **per input row** (the driver loop in
//! `executor/write.rs`), and each call re-plans: a fresh `LogicalPlan` and a
//! full DataFusion planning pass, which the session-local plan cache cannot
//! help because the plan is rebuilt each time. So a slow batched MERGE has two
//! candidate causes — a bad anchor, and per-row replanning — and they are fixed
//! by different changes. A benchmark reporting only total time cannot say which
//! one it measured, and "it got faster" would be consistent with either.
//!
//! `rows_scanned` was meant to separate them, and **cannot**: it is blind to
//! MERGE's per-row plans. Every arm below reports exactly the rows its *outer*
//! MATCH examined — 790 for the two-MATCH arms, 395 for the one-MATCH arms —
//! including an arm that takes sixteen seconds and one that takes a tenth of
//! one. Reading it here would "prove" there is no scan problem. The column is
//! printed anyway, because a constant where a signal belongs is worth seeing.
//!
//! What settles it instead needs no instrument: hold the batch fixed and vary
//! the size of the label. A per-row scan of the label is proportional to it; an
//! anchored lookup is not. That is what this probe sweeps, and it is the only
//! thing the verdict reads.
//!
//! # Arms
//!
//! Every arm creates the same edges between the same endpoints, and the count
//! created is asserted equal before any cost is compared — an arm doing less
//! work would make the table meaningless.
//!
//! 1. `both bound`   — both endpoints matched before the MERGE. The shape
//!    `mutation_benchmarks` already covers, and the case the walk handles well.
//! 2. `bound first`  — one endpoint bound, written at the head of the pattern.
//!    The walk starts on it.
//! 3. `bound last`   — the same single endpoint bound, written at the *tail*.
//!    Semantically identical to arm 2; only the arrow direction differs. This is
//!    #225.
//! 4. `MATCH+CREATE` — the floor. No MERGE, so no per-row `execute_merge_match`
//!    and no per-row replanning; it isolates how much of arm 2 is MERGE
//!    machinery rather than the work itself.
//!
//! ```text
//! cargo run --release -p uni-db --example merge_anchor_probe
//! ```

use std::collections::HashMap;
use std::time::Instant;

use uni_db::{Uni, Value};

fn batch_size() -> usize {
    std::env::var("MERGE_PROBE_BATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400)
}

struct Arm {
    label: &'static str,
    /// Every arm links `r.src -> r.dst`, differing only in how it is written.
    query: &'static str,
}

const ARMS: &[Arm] = &[
    Arm {
        label: "1  both bound        MATCH a, b  MERGE (a)->(b)",
        query: "UNWIND $batch AS r \
                MATCH (a:Entity {uid: r.src}), (b:Entity {uid: r.dst}) \
                MERGE (a)-[e:OWNS]->(b)",
    },
    Arm {
        label: "2  bound FIRST       MATCH a     MERGE (a)->(b{…})",
        query: "UNWIND $batch AS r \
                MATCH (a:Entity {uid: r.src}) \
                MERGE (a)-[e:OWNS]->(b:Entity {uid: r.dst})",
    },
    Arm {
        label: "3  bound LAST        MATCH b     MERGE (a{…})->(b)",
        query: "UNWIND $batch AS r \
                MATCH (b:Entity {uid: r.dst}) \
                MERGE (a:Entity {uid: r.src})-[e:OWNS]->(b)",
    },
    // The issue's own example is a *bare* far node — `MERGE (a)<-[:R]-(b)` with
    // no label and no property map. That is the shape its ScanAll claim is
    // about, and it is materially different from arm 3: an inline `{uid: …}`
    // reaches the scan's filter and an index, so arm 3 never produces the
    // unbounded scan even though it is written the "wrong" way round. These two
    // remove the inline map, then the label as well, so the claim is tested as
    // filed rather than as paraphrased.
    Arm {
        label: "3b bound LAST, label only   MERGE (a:Entity)->(b)",
        query: "UNWIND $batch AS r \
                MATCH (b:Entity {uid: r.dst}) \
                MERGE (a:Entity)-[e:OWNS]->(b)",
    },
    Arm {
        label: "3c bound LAST, bare node    MERGE (a)->(b)",
        query: "UNWIND $batch AS r \
                MATCH (b:Entity {uid: r.dst}) \
                MERGE (a)-[e:OWNS]->(b)",
    },
    // Same as arm 1 but with an *anonymous* relationship. The only difference
    // is `[e:OWNS]` vs `[:OWNS]`, and `merge_relationship_fastpath_shape`
    // rejects a pattern whose relationship carries a variable — so this is the
    // one arm that can take the fastpath.
    Arm {
        label: "1b both bound, ANON edge   MERGE (a)-[:OWNS]->(b)",
        query: "UNWIND $batch AS r \
                MATCH (a:Entity {uid: r.src}), (b:Entity {uid: r.dst}) \
                MERGE (a)-[:OWNS]->(b)",
    },
    Arm {
        label: "4  MATCH+CREATE      (the floor, no MERGE)",
        query: "UNWIND $batch AS r \
                MATCH (a:Entity {uid: r.src}), (b:Entity {uid: r.dst}) \
                CREATE (a)-[e:OWNS]->(b)",
    },
];

/// A deterministic LCG, so the graph and the batch are identical run to run.
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
}

fn batch_value(rng: &mut Rng, nodes: usize) -> Value {
    Value::List(
        (0..batch_size())
            .map(|_| {
                let (s, d) = (rng.below(nodes), rng.below(nodes));
                Value::Map(HashMap::from([
                    ("src".to_string(), Value::String(format!("e{s}"))),
                    ("dst".to_string(), Value::String(format!("e{d}"))),
                ]))
            })
            .collect(),
    )
}

/// Label sizes to sweep. The batch is held fixed, so an arm whose cost tracks
/// this is scanning the label per input row.
const SCALES: &[usize] = &[5_000, 20_000];

async fn build(root: &str, nodes: usize) -> Result<Uni, Box<dyn std::error::Error>> {
    let db = Uni::open(root.to_string()).build().await?;
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
    drop(db);

    // Reopen: `BulkWriter` rows are absent from `VidLabelsIndex` on the
    // inserting handle (#269), which escalates batched vertex-property reads to
    // a scan of every declared label (#264) — a fan-out that would land on top
    // of the effect under test.
    Ok(Uni::open(root.to_string()).build().await?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let batch_n = batch_size();
    // `[arm][scale]`, so growth is read across a row.
    let mut grid: Vec<Vec<(f64, usize, usize)>> = ARMS.iter().map(|_| Vec::new()).collect();

    for &nodes in SCALES {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("g").to_string_lossy().into_owned();
        let db = build(&root, nodes).await?;
        println!("=== {nodes} vertices, uid indexed, batch = {batch_n} rows ===");
        println!(
            "  {:<50} {:>9} {:>14} {:>8}",
            "arm", "time", "rows_scanned", "created"
        );
        for (i, arm) in ARMS.iter().enumerate() {
            let mut rng = Rng(11);
            let batch = batch_value(&mut rng, nodes);
            // Rolled back, not committed: no arm may see edges an earlier one
            // created, which would turn a MERGE insert into a MERGE match.
            let tx = db.session().tx().await?;
            let started = Instant::now();
            let res = tx
                .execute_with(arm.query)
                .param("batch", batch)
                .run()
                .await?;
            let secs = started.elapsed().as_secs_f64();
            let m = (
                secs,
                res.metrics().rows_scanned,
                res.relationships_created(),
            );
            tx.rollback();
            println!("  {:<50} {:>8.3}s {:>14} {:>8}", arm.label, m.0, m.1, m.2);
            grid[i].push(m);
        }
        println!();
    }

    // ---- the specific-pair arms must be doing the same work ----------------
    //
    // Arms 3b and 3c ask a different question — "any node pointing at b", not
    // "this specific node" — so a row can match an edge an earlier row created
    // and they legitimately create fewer.
    for (scale, &nodes) in SCALES.iter().enumerate() {
        let created = grid[0][scale].2;
        for i in [0usize, 1, 2, 5, 6] {
            assert_eq!(
                grid[i][scale].2, created,
                "at {nodes} nodes arm `{}` created {} relationships, not \
                 {created}; the arms are not comparable",
                ARMS[i].label, grid[i][scale].2
            );
        }
        assert!(
            created > 0,
            "no relationships created — the fixture is inert"
        );
    }

    // ---- verdict: growth with the label, at a fixed batch -------------------
    let lo = 0;
    let hi = SCALES.len() - 1;
    let factor = SCALES[hi] as f64 / SCALES[lo] as f64;
    println!("cost growth for a {factor:.0}x larger label at a fixed {batch_n}-row batch");
    println!("  {:<50} {:>10}", "arm", "growth");
    let mut growth = Vec::new();
    for (i, arm) in ARMS.iter().enumerate() {
        let g = grid[i][hi].0 / grid[i][lo].0.max(1e-9);
        println!("  {:<50} {g:>9.2}x", arm.label);
        growth.push(g);
    }

    // A per-row label scan grows with the label; an anchored lookup does not.
    // The threshold sits well below the {factor}x a fully proportional scan
    // would show and well above the noise a flat arm produces.
    let scanning: Vec<&str> = ARMS
        .iter()
        .enumerate()
        .filter(|(i, _)| growth[*i] > 2.0)
        .map(|(_, a)| a.label)
        .collect();
    println!();
    if scanning.is_empty() {
        println!("  No arm's cost tracks the label size: every spelling anchors.");
    } else {
        println!("  These spellings scan the label once per input row (#225):");
        for label in &scanning {
            println!("    - {label}");
        }
        println!(
            "  A bare unlabeled MERGE node is expected here and cannot be fixed by\n               anchoring: there is nothing to look it up by, and moving it off the\n               head of the walk is rejected by the traversal-target check."
        );
    }
    Ok(())
}
