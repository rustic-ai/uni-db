//! Does a Locy rule-body `WHERE` reach the scan, or only a filter above it? (#226)
//!
//! #226 says a rule-body `WHERE` is hand-built into a `LogicalPlan::Filter`
//! above the pattern (`locy_planner.rs:1200-1206`) instead of going through
//! `plan_where_clause`, which is what pushes a predicate into `Scan.filter`
//! (`planner.rs:7120-7166`). Index selection reads *only* `Scan.filter`
//! (`df_planner.rs:1761`), so a predicate that lands in a `Filter` node above
//! the scan consults no index and filters a fully-materialised scan.
//!
//! If that is right, the same predicate costs wildly different amounts
//! depending only on where it is written:
//!
//! ```text
//! MATCH (e:Entity) WHERE e.uid = 'e42'   -- rule-body WHERE: full label scan?
//! MATCH (e:Entity {uid: 'e42'})          -- inline map: reaches Scan.filter
//! ```
//!
//! # The instrument is `rows_scanned`
//!
//! `QueryMetrics::rows_scanned` counts rows examined by scans before filtering,
//! so it reads the plan's choice directly instead of inferring it from a
//! duration. An index seek examines the matching rows; a scan-and-filter
//! examines the table.
//!
//! It did not work for Locy when this probe was written: every scan in a rule
//! body runs under `execute_subplan_with_outer_vars`, which rebuilt the graph
//! context without carrying the counters over, so a rule evaluation reported 0
//! however much it scanned. This probe is what found that — it was written to
//! use the counter, read zero on every Locy arm, and its first verdict said the
//! opposite of what the timings showed. Fixed since; the column is real on both
//! kinds of arm now.
//!
//! Timings are reported beside it and are never the finding alone. Each block
//! runs one predicate over one graph in one process, differing in a single
//! place, and asserts the arms return identical rows before any comparison is
//! made — so a ratio between them is a property of the plans, not the machine.
//!
//! # Two questions, two blocks of arms
//!
//! **Block 1 — the claim itself**, on a unique indexed property, where an index
//! seek and a full scan differ by the size of the table. Arms A (Cypher) and C
//! (Locy, inline map) are the controls: both are known to reach `Scan.filter`,
//! so if B (Locy, rule-body WHERE) matches them the claim is dead, and if it
//! scans the label while they do not, the claim holds.
//!
//! **Block 2 — a live residual I already measured.** The #267 probe found every
//! Locy arm costing ~6.6x the equivalent plain Cypher over an identical MATCH,
//! and I recorded that as an open question rather than a finding. The
//! reporter's rule filters on `o.blocked = true, e.blocked = false` — exactly
//! the shape #226 says never reaches the scan. Block 2 re-runs that comparison
//! with `blocked` **indexed**, which is the cheap decisive test: if Locy ignores
//! the index while Cypher uses it, #226 is the cause of the residual; if both
//! move together, it is not, and the residual is Locy runtime overhead that
//! needs its own explanation.
//!
//! Note that `blocked` is deliberately *low* selectivity (~13%). An index helps
//! far less there than on `uid`, which is the point: it separates "the index is
//! consulted" from "the index is worth consulting".
//!
//! ```text
//! cargo run --release -p uni-db --example locy_where_pushdown_probe
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use uni_db::locy::LocyConfig;
use uni_db::{Uni, Value};

const N_NODES: usize = 40_000;
const N_EDGES: usize = 80_000;

/// The `uid` every block-1 arm selects. Its exact value does not matter, only
/// that it identifies one row.
const TARGET: &str = "e17321";

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

struct Arm {
    label: &'static str,
    /// `None` for the plain-Cypher control, which does not go through Locy.
    locy: Option<String>,
    cypher: Option<&'static str>,
}

fn arms() -> Vec<Arm> {
    vec![
        // ---- block 1: a unique, indexed property -------------------------
        Arm {
            label: "A  cypher   WHERE e.uid = …",
            locy: None,
            cypher: Some("MATCH (e:Entity) WHERE e.uid = 'e17321' RETURN e.uid AS uid"),
        },
        Arm {
            label: "B  locy     WHERE e.uid = …   (rule body)",
            locy: Some(format!(
                "CREATE RULE r AS MATCH (e:Entity) WHERE e.uid = '{TARGET}' \
                 YIELD KEY e.uid AS uid \nQUERY r RETURN uid"
            )),
            cypher: None,
        },
        Arm {
            label: "C  locy     (e:Entity {uid: …})  inline",
            locy: Some(format!(
                "CREATE RULE r AS MATCH (e:Entity {{uid: '{TARGET}'}}) \
                 YIELD KEY e.uid AS uid \nQUERY r RETURN uid"
            )),
            cypher: None,
        },
        // ---- block 2: the #267 residual, on an indexed low-selectivity
        // boolean, written all three ways over the same traversal ----------
        Arm {
            label: "D  cypher   blocked predicates",
            locy: None,
            cypher: Some(
                "MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
                 WHERE o.blocked = true AND e.blocked = false \
                 RETURN e.uid AS uid, sum(s.pct) AS agg",
            ),
        },
        Arm {
            label: "E  locy     blocked  (rule body WHERE)",
            locy: Some(
                "CREATE RULE r AS MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
                 WHERE o.blocked = true, e.blocked = false \
                 FOLD agg = MSUM(s.pct) \
                 YIELD KEY e.uid AS uid, agg \nQUERY r RETURN uid, agg"
                    .to_string(),
            ),
            cypher: None,
        },
        Arm {
            label: "F  locy     blocked  (inline maps)",
            locy: Some(
                "CREATE RULE r AS \
                 MATCH (o:Entity {blocked: true})-[s:OWNS]->(e:Entity {blocked: false}) \
                 FOLD agg = MSUM(s.pct) \
                 YIELD KEY e.uid AS uid, agg \nQUERY r RETURN uid, agg"
                    .to_string(),
            ),
            cypher: None,
        },
    ]
}

struct Measurement {
    seconds: f64,
    rows: usize,
    scanned: usize,
}

async fn load(root: &str) -> Result<Uni, Box<dyn std::error::Error>> {
    let db = Uni::open(root.to_string()).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (uid STRING, name STRING, blocked BOOL)")
        .await?;
    tx.execute("CREATE EDGE TYPE OWNS (pct FLOAT) FROM Entity TO Entity")
        .await?;
    // Both predicates are indexed. `blocked` carrying an index is the whole
    // point of block 2 — without it, "Locy ignored the index" and "there was no
    // index to ignore" are the same observation.
    tx.execute("CREATE INDEX idx_uid FOR (e:Entity) ON (e.uid)")
        .await?;
    tx.execute("CREATE INDEX idx_blocked FOR (e:Entity) ON (e.blocked)")
        .await?;
    tx.commit().await?;

    let mut rng = Rng(7);
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..N_NODES)
        .map(|i| {
            HashMap::from([
                ("uid".to_string(), Value::String(format!("e{i}"))),
                (
                    "name".to_string(),
                    Value::String(format!("Entity Number {i}")),
                ),
                ("blocked".to_string(), Value::Bool(rng.unit() < 0.13)),
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("Entity", vertices).await?;

    let mut edges = Vec::with_capacity(N_EDGES);
    for _ in 0..N_EDGES {
        let (a, b) = (rng.below(N_NODES), rng.below(N_NODES));
        if a != b {
            edges.push(uni_bulk::EdgeData::new(
                vids[a],
                vids[b],
                HashMap::from([("pct".to_string(), Value::Float(5.0 + rng.unit() * 95.0))]),
            ));
        }
    }
    bulk.insert_edges("OWNS", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    drop(db);

    // Reopen: `BulkWriter` rows are absent from `VidLabelsIndex` on the
    // inserting handle (#269), which escalates a batched vertex-property read
    // to a scan of every declared label (#264) — a fan-out that would land on
    // top of the counter under test and confound exactly the arms being
    // compared.
    Ok(Uni::open(root.to_string()).build().await?)
}

async fn run_arm(db: &Uni, arm: &Arm) -> Result<Measurement, Box<dyn std::error::Error>> {
    if let Some(program) = &arm.locy {
        let config = LocyConfig {
            max_iterations: 100,
            timeout: Duration::from_secs(300),
            ..Default::default()
        };
        let started = Instant::now();
        let result = db
            .session()
            .locy_with(program)
            .with_config(config)
            .run()
            .await?;
        let seconds = started.elapsed().as_secs_f64();
        let scanned = result.metrics().rows_scanned;
        let (inner, _) = result.into_parts();
        return Ok(Measurement {
            seconds,
            rows: inner.rows().map(|r| r.len()).unwrap_or(0),
            scanned,
        });
    }
    let query = arm.cypher.expect("an arm is either locy or cypher");
    let started = Instant::now();
    let r = db.session().query(query).await?;
    Ok(Measurement {
        seconds: started.elapsed().as_secs_f64(),
        rows: r.rows().len(),
        scanned: r.metrics().rows_scanned,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("g").to_string_lossy().into_owned();
    let started = Instant::now();
    let db = load(&root).await?;
    println!(
        "loaded {N_NODES} vertices / ~{N_EDGES} OWNS edges in {:.1}s; uid and blocked are indexed\n",
        started.elapsed().as_secs_f64()
    );

    println!(
        "  {:<44} {:>9} {:>13} {:>8}",
        "arm", "time", "rows_scanned", "rows"
    );
    let all = arms();
    let mut out: Vec<Measurement> = Vec::new();
    for arm in &all {
        // Two runs, report the faster: the read plan cache is session-local, so
        // a first run pays planning the others do not.
        let mut best: Option<Measurement> = None;
        for _ in 0..2 {
            let m = run_arm(&db, arm).await?;
            if best.as_ref().is_none_or(|b| m.seconds < b.seconds) {
                best = Some(m);
            }
        }
        let m = best.expect("at least one run");
        println!(
            "  {:<44} {:>8.3}s {:>13} {:>8}",
            arm.label, m.seconds, m.scanned, m.rows
        );
        out.push(m);
    }

    // ---- the arms in a block must answer the same question ----------------
    assert_eq!(
        (out[0].rows, out[1].rows, out[2].rows),
        (1, 1, 1),
        "block 1 arms must each return exactly the one target row; \
         they are otherwise not comparable"
    );
    assert_eq!(
        out[4].rows, out[5].rows,
        "block 2's two Locy spellings returned {} and {} rows; the predicate \
         means something different written inline, so the scan counts below \
         would not be comparable",
        out[4].rows, out[5].rows
    );

    // ---- verdict ----------------------------------------------------------
    //
    // The verdicts read time rather than `rows_scanned`, which is now populated
    // on both kinds of arm and printed in the table above. That is deliberate:
    // pushdown moves rows out of `FilterExec` and into the scan's own predicate,
    // and on a full-label scan of a low-selectivity boolean the rows *examined*
    // barely move while the cost does. Rows examined is the right instrument for
    // "did the planner pick a seek or a scan" (block 1); cost is the right one
    // for "was the predicate evaluated in the cheap place" (block 2). Every
    // comparison below is between arms already asserted to return identical
    // rows.
    let (cypher, locy_where, locy_inline) = (&out[0], &out[1], &out[2]);
    println!("\nblock 1 — a unique indexed property (1 row of {N_NODES})");
    println!("  cypher WHERE        {:>8.3}s", cypher.seconds);
    println!("  locy   WHERE        {:>8.3}s", locy_where.seconds);
    println!("  locy   inline map   {:>8.3}s", locy_inline.seconds);
    println!(
        "  rule-body WHERE costs {:.1}x the same predicate written inline",
        locy_where.seconds / locy_inline.seconds.max(1e-9)
    );

    let (d, e, f) = (&out[3], &out[4], &out[5]);
    println!("\nblock 2 — the #267 residual, on an indexed low-selectivity boolean");
    println!(
        "  cypher            {:>8.3}s   ({} rows examined)",
        d.seconds, d.scanned
    );
    println!("  locy rule WHERE   {:>8.3}s", e.seconds);
    println!("  locy inline map   {:>8.3}s", f.seconds);
    let where_ratio = e.seconds / d.seconds.max(1e-9);
    let inline_ratio = f.seconds / d.seconds.max(1e-9);
    println!("\n  vs cypher: {where_ratio:.1}x (rule WHERE) vs {inline_ratio:.1}x (inline)");

    // The discriminator. If the rule-body WHERE were merely Locy overhead, both
    // Locy spellings would sit at the same multiple of Cypher, since they run
    // the identical fixpoint over the identical rows. They do not: only the
    // spelling that has to reach `Scan.filter` to be cheap is expensive.
    if where_ratio > 2.0 && inline_ratio < where_ratio / 2.0 {
        println!(
            "\n  REPRODUCED (#226): moving the identical predicate from the rule body\n  \
             into an inline map is worth {:.1}x, and lands Locy within {inline_ratio:.1}x of\n  \
             plain Cypher. The cost is where the predicate is written, not the\n  \
             Locy runtime — which also accounts for the residual #267 left open.",
            e.seconds / f.seconds.max(1e-9)
        );
    } else if where_ratio > 2.0 {
        println!(
            "\n  Locy is {where_ratio:.1}x Cypher in BOTH spellings, so #226 does not explain\n  \
             it — the cost is elsewhere in the Locy runtime and needs its own account."
        );
    } else {
        println!("\n  NOT REPRODUCED: Locy and Cypher are comparable either way.");
    }
    Ok(())
}
