//! Does an indexed predicate on the pattern's right-hand node reach the anchor? (#268)
//!
//! #268 reports that two semantically identical patterns differ by orders of
//! magnitude, on nothing but the direction the arrow is written:
//!
//! ```text
//! MATCH (o:Entity)-[s:OWNS]->(e:Entity) WHERE e.uid = $u   -- reported >25s
//! MATCH (e:Entity)<-[s:OWNS]-(o:Entity) WHERE e.uid = $u   -- reported 0.04s
//! ```
//!
//! The report was measured on the `v3.4.0` wheel. Two anchoring commits landed
//! *after* that tag — `0c4f2cb32` (#219, `reversed_for_bound_anchor`) and
//! `08e41d890` (#224, `split_at_bound_anchor`) — so the reported behaviour has
//! to be re-established on `main` before it is worth designing against. That is
//! this probe's first job. Its second is to check the arms those commits *do*
//! reach, which the reporter could not have observed.
//!
//! # The instrument: `rows_scanned`, not wall-clock
//!
//! Wall-clock alone cannot tell "the planner chose a full scan" from a slow
//! machine, and this repository has repeatedly attributed a cost to a mechanism
//! the query did not use. `QueryMetrics::rows_scanned` is "rows examined by
//! scans, before filtering and projection", so it reads the *plan's* choice
//! directly: an index seek examines the matching rows, a scan-and-filter
//! examines the table. The timing column is reported beside it as corroboration,
//! never as the finding.
//!
//! Every arm returns the same rows, and that is asserted rather than assumed —
//! two arms doing different amounts of work would make the comparison
//! meaningless, which is the failure mode a bare timing table hides.
//!
//! # Arms
//!
//! 1. `select only`   — the indexed lookup on its own; the floor.
//! 2. `forward`       — traversing *out* of the filtered node; already fast.
//! 3. `reverse, filtered node written first`  — the spelling that works.
//! 4. `reverse, filtered node written last`   — #268's defect.
//! 5. `reverse, inline {uid: $u}`             — the report says this is slow too.
//! 6. `reverse, split across two MATCHes`     — binds `e` in a prior clause, so
//!    `reversed_for_bound_anchor` should fire. The report says this timed out;
//!    on `main` it should not. This arm is the one the post-tag commits changed.
//!
//! # Why the store is reopened after loading
//!
//! `BulkWriter` rows are absent from `VidLabelsIndex` on the inserting handle
//! (#269), which escalates a batched vertex-property read to a scan of every
//! declared label (#264) — a per-read fan-out that would land on top of the
//! effect under test and confound it. Reopening rebuilds the index from the
//! main vertex table. The reporter's script reopens for the same class of
//! reason. With one label declared here the fan-out is nearly free either way,
//! but the reopen removes the question rather than arguing about it.

use std::collections::HashMap;
use std::time::Instant;

use uni_db::{Uni, Value};

const N_NODES: usize = 60_000;
const N_EDGES: usize = 120_000;

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
    query: &'static str,
}

/// The six spellings. Arms 3-6 must all return the in-degree of the target.
const ARMS: &[Arm] = &[
    Arm {
        label: "select only",
        query: "MATCH (e:Entity) WHERE e.uid = $u RETURN e.name AS x",
    },
    Arm {
        label: "forward  (e)-[:OWNS]->(a)",
        query: "MATCH (e:Entity)-[s:OWNS]->(a:Entity) WHERE e.uid = $u RETURN a.uid AS x",
    },
    Arm {
        label: "reverse  (e)<-[:OWNS]-(o)   filtered node FIRST",
        query: "MATCH (e:Entity)<-[s:OWNS]-(o:Entity) WHERE e.uid = $u RETURN o.uid AS x",
    },
    Arm {
        label: "reverse  (o)-[:OWNS]->(e)   filtered node LAST",
        query: "MATCH (o:Entity)-[s:OWNS]->(e:Entity) WHERE e.uid = $u RETURN o.uid AS x",
    },
    Arm {
        label: "reverse  (o)-[:OWNS]->(e {uid:$u})  inline",
        query: "MATCH (o:Entity)-[s:OWNS]->(e:Entity {uid: $u}) RETURN o.uid AS x",
    },
    Arm {
        label: "reverse  split across two MATCHes",
        query: "MATCH (e:Entity) WHERE e.uid = $u \
                MATCH (o:Entity)-[s:OWNS]->(e) RETURN o.uid AS x",
    },
    // Controls on an UNINDEXED property, to keep a fix honest. `name` is not
    // indexed, and `Entity Number <i>` identifies exactly the same node as
    // `e<i>`, so these two arms differ from arms 3 and 4 only in whether an
    // index backs the predicate.
    //
    // They decide something a fix depends on. "Anchor on whichever node carries
    // a bound predicate" and "anchor on whichever node carries an *indexed*
    // bound predicate" are different rules, and they are indistinguishable on
    // the arms above. If the unindexed pair is symmetric and slow at both ends,
    // the operative thing is the index and the anchor rule has to consult the
    // schema; if the first-written arm is fast without an index, then written
    // order alone is doing work and the cheaper rule would suffice.
    Arm {
        label: "unindexed (e)<-[:OWNS]-(o)  filtered node FIRST",
        query: "MATCH (e:Entity)<-[s:OWNS]-(o:Entity) WHERE e.name = $n RETURN o.uid AS x",
    },
    Arm {
        label: "unindexed (o)-[:OWNS]->(e)  filtered node LAST",
        query: "MATCH (o:Entity)-[s:OWNS]->(e:Entity) WHERE e.name = $n RETURN o.uid AS x",
    },
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("g").to_string_lossy().into_owned();

    // ---- load -------------------------------------------------------------
    let build_started = Instant::now();
    let db = Uni::open(root.clone()).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (uid STRING, name STRING)")
        .await?;
    tx.execute("CREATE EDGE TYPE OWNS FROM Entity TO Entity")
        .await?;
    tx.execute("CREATE INDEX idx_uid FOR (e:Entity) ON (e.uid)")
        .await?;
    tx.commit().await?;

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
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("Entity", vertices).await?;

    // Skewed in-degree, like a real ownership register: a few holding companies
    // are owned by many parties, most entities by one or two. The reporter notes
    // a uniform random graph understates the effect considerably.
    let mut rng = Rng(11);
    let hubs: Vec<usize> = (0..N_NODES / 200).map(|_| rng.below(N_NODES)).collect();
    let mut edges = Vec::with_capacity(N_EDGES);
    for _ in 0..N_EDGES {
        let a = rng.below(N_NODES);
        let b = if rng.unit() < 0.5 {
            hubs[rng.below(hubs.len())]
        } else {
            rng.below(N_NODES)
        };
        if a != b {
            // No edge properties: no arm reads one, so carrying them would only
            // add a variable to the measurement.
            edges.push(uni_bulk::EdgeData::new(vids[a], vids[b], HashMap::new()));
        }
    }
    let n_edges = edges.len();
    bulk.insert_edges("OWNS", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    drop(db);

    // Reopen: see the module note on #269/#264.
    let db = Uni::open(root.clone()).build().await?;
    let session = db.session();
    println!(
        "loaded {N_NODES} vertices / {n_edges} OWNS edges in {:.1}s; uid is indexed",
        build_started.elapsed().as_secs_f64()
    );

    // ---- target: the highest in-degree node, found with the fast spelling ---
    let top = session
        .query(
            "MATCH (e:Entity)<-[:OWNS]-(o:Entity) \
             RETURN e.uid AS u, count(*) AS n ORDER BY n DESC LIMIT 1",
        )
        .await?;
    let uid: String = top.rows()[0].get("u")?;
    let in_degree: i64 = top.rows()[0].get("n")?;
    // The same node by its unindexed property, for the controls.
    let name: String = session
        .query_with("MATCH (e:Entity) WHERE e.uid = $u RETURN e.name AS nm")
        .param("u", Value::String(uid.clone()))
        .fetch_all()
        .await?
        .rows()[0]
        .get("nm")?;
    println!("target: {uid} (in-degree {in_degree})\n");

    // ---- arms --------------------------------------------------------------
    println!(
        "  {:<50} {:>9} {:>13} {:>8}",
        "spelling", "time", "rows_scanned", "rows"
    );
    let mut results: Vec<(usize, f64, usize)> = Vec::new();
    for arm in ARMS {
        // Two runs, report the faster: the read plan cache is session-local, so
        // the first run of each arm pays planning the others do not.
        let mut best = f64::MAX;
        let mut scanned = 0usize;
        let mut rows = 0usize;
        for _ in 0..2 {
            let started = Instant::now();
            let r = session
                .query_with(arm.query)
                .param("u", Value::String(uid.clone()))
                .param("n", Value::String(name.clone()))
                .fetch_all()
                .await?;
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            if ms < best {
                best = ms;
            }
            scanned = r.metrics().rows_scanned;
            rows = r.rows().len();
        }
        // Plan-level corroboration: `rows_scanned` says how much was examined,
        // `index_usage` says what the planner decided. Reporting both means a
        // finding does not rest on a counter whose population could itself
        // regress.
        let usage = session
            .query_with(arm.query)
            .param("u", Value::String(uid.clone()))
            .param("n", Value::String(name.clone()))
            .explain()
            .await
            .map(|e| format!("{:?}", e.index_usage))
            .unwrap_or_else(|e| format!("explain failed: {e}"));
        println!(
            "  {:<50} {best:>8.1}ms {scanned:>13} {rows:>8}   {}",
            arm.label,
            &usage[..usage.len().min(90)]
        );
        results.push((rows, best, scanned));
    }

    // ---- the arms must be answering the same question ----------------------
    let expected = in_degree as usize;
    for (i, arm) in ARMS.iter().enumerate().skip(2) {
        assert_eq!(
            results[i].0, expected,
            "arm `{}` returned {} rows, not the target's in-degree {expected}; \
             the arms are not comparable",
            arm.label, results[i].0
        );
    }

    // ---- verdict -----------------------------------------------------------
    let fast = &results[2];
    let last = &results[3];
    let inline = &results[4];
    let split = &results[5];
    println!(
        "\n  scan ratio, filtered-node-LAST vs FIRST: {:.0}x rows examined",
        last.2 as f64 / fast.2.max(1) as f64
    );
    println!("  time ratio: {:.0}x", last.1 / fast.1.max(1e-9));

    let defect = last.2 > 10 * fast.2.max(1);
    println!();
    if defect {
        println!("REPRODUCED on main: writing the filtered node last makes the planner");
        println!("  examine the whole table where writing it first uses the index.");
    } else {
        println!("NOT REPRODUCED on main: both reverse spellings examine comparable rows.");
    }
    println!(
        "  inline {{uid:$u}}      : {} the index ({} rows examined)",
        if inline.2 > 10 * fast.2.max(1) {
            "MISSES"
        } else {
            "uses"
        },
        inline.2
    );
    println!(
        "  split across MATCHes : {} the index ({} rows examined)",
        if split.2 > 10 * fast.2.max(1) {
            "MISSES"
        } else {
            "uses"
        },
        split.2
    );
    Ok(())
}
