//! Is `YIELD KEY <node>` quadratic where `YIELD KEY <node>.<prop>` is linear? (#267)
//!
//! #267 reports that a **non-recursive** Locy rule costs ~140x more when its
//! KEY column is a node than when it is a scalar property of that same node,
//! and that the node form scales quadratically (4x edges -> ~16x time) where
//! the scalar form scales linearly.
//!
//! ```text
//! FOLD agg = MSUM(s.pct)
//! YIELD KEY e,             agg   -- reported 125.78s at 124,845 edges
//! YIELD KEY e.uid AS uid,  agg   -- reported   0.90s at the same scale
//! ```
//!
//! # What this probe decided
//!
//! **The reported mechanism is not the one.** The report blames `RowDedupState`
//! falling back to `FixpointState::compute_delta_legacy` — "rebuild a
//! `HashSet<Vec<ScalarKey>>` from all facts each call" — because a node column
//! is an Arrow struct that `RowConverter` will not accept. Two things are wrong
//! with that, both checkable without running anything:
//!
//! 1. A node-valued KEY is not a struct. `plan_locy_project` projects the
//!    variable's `{var}._vid` column, so the KEY column is a plain `UInt64`
//!    VID, which `RowConverter` accepts. The fallback never engages.
//! 2. The rule is non-recursive, and a non-recursive stratum never builds a
//!    `FixpointState` at all. `compute_delta_legacy` is not called once.
//!
//! The path named is real and really is quadratic; it is simply not on this
//! query's route. It was found by reading a rustdoc comment that matched the
//! symptom's shape, which is why the probe measures a discriminator rather than
//! taking it on: a per-call rebuild can only be quadratic if the call count
//! grows with the data, and `stats.total_iterations` reads 1 at every scale.
//!
//! **What actually fixed it** was `5a8dd387b`, "decode a UDF argument once per
//! run of equal rows", filed against #229/#245 and closing this incidentally.
//! `invoke_cypher_udf` rebuilt every argument as a `Value` per row; a node
//! arrives as a tagged CypherValue in a `LargeBinary` column, whose decode is a
//! full msgpack walk, where a scalar `uid` is a primitive read. Bisected over
//! `v3.4.0..HEAD` with the reporter's own script as the predicate, eight steps,
//! no skips, boundary confirmed against the direct parent:
//!
//! ```text
//! edges     89c9b84f6 (parent)   5a8dd387b (fix)
//!  7 998         0.22s               0.08s
//! 15 997         0.82s               0.17s
//! 31 997         2.72s               0.55s
//! penalty        12x                 2x
//! exponent       1.90                1.09
//! ```
//!
//! What is *not* established: why the per-row decode was super-linear rather
//! than merely expensive. A constant per-row cost over a row count that grows
//! linearly would give a linear curve, so something in the decoded argument
//! grows with the graph. That is a live question, not a finding.
//!
//! # Instruments
//!
//! - **`stats.total_iterations`** — the discriminator described above. Without
//!   this column a timing table cannot separate "more iterations" from "more
//!   work per iteration", and the mechanism in the report would have been
//!   adopted on the strength of sounding right.
//! - **the scaling exponent**, not a single ratio. `log(t2/t1) / log(n2/n1)`
//!   reads ~1 for linear and ~2 for quadratic. One ratio at one size cannot
//!   distinguish "quadratic" from "linear with a large constant", and those two
//!   do not have the same fix.
//! - **wall-clock**, reported beside them and never as the finding on its own.
//!
//! # Arms
//!
//! Every arm runs the same MATCH over the same graph. `NodeKey` and `ScalarKey`
//! differ in one word. The `NoFold` pair tests the report's control that the
//! aggregate is not the variable, and `Cypher` is the floor — the same
//! aggregation with no Locy runtime under it.
//!
//! The QUERY projection is held at `agg` alone wherever the shape allows, so
//! that hydrating a node for output is not folded into the KEY measurement.
//! `NodeKeyProjected` restores the reporter's `RETURN e.name` to check whether
//! the projection carries any of the cost.
//!
//! ```text
//! cargo run --release -p uni-db --example locy_key_node_probe
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use uni_db::{Uni, Value};

/// Edge counts to sweep. Each is 2x the last, so the exponent is read off
/// consecutive pairs. The reporter notes their synthetic graph degenerates
/// above ~32k edges for reasons unrelated to the defect (plain Cypher itself
/// slows by two orders of magnitude there), so the sweep stops below that.
const SCALES: &[usize] = &[8_000, 16_000, 32_000];

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
    /// `None` for the plain-Cypher floor, which does not go through Locy.
    locy: Option<&'static str>,
    cypher: Option<&'static str>,
}

const ARMS: &[Arm] = &[
    Arm {
        label: "FOLD, KEY e         (node)",
        locy: Some(
            "CREATE RULE r AS \
             MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o.blocked = true, e.blocked = false \
             FOLD agg = MSUM(s.pct) \
             WHERE agg >= 50.0 \
             YIELD KEY e, agg \
             QUERY r RETURN agg",
        ),
        cypher: None,
    },
    Arm {
        label: "FOLD, KEY e.uid     (scalar)",
        locy: Some(
            "CREATE RULE r AS \
             MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o.blocked = true, e.blocked = false \
             FOLD agg = MSUM(s.pct) \
             WHERE agg >= 50.0 \
             YIELD KEY e.uid AS uid, agg \
             QUERY r RETURN agg",
        ),
        cypher: None,
    },
    Arm {
        label: "FOLD, KEY e, RETURN e.name",
        locy: Some(
            "CREATE RULE r AS \
             MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o.blocked = true, e.blocked = false \
             FOLD agg = MSUM(s.pct) \
             WHERE agg >= 50.0 \
             YIELD KEY e, agg \
             QUERY r RETURN e.name AS name, agg",
        ),
        cypher: None,
    },
    Arm {
        label: "no FOLD, KEY e      (node)",
        locy: Some(
            "CREATE RULE r AS \
             MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o.blocked = true, e.blocked = false \
             YIELD KEY e \
             QUERY r RETURN e.uid AS uid",
        ),
        cypher: None,
    },
    Arm {
        label: "no FOLD, KEY e.uid  (scalar)",
        locy: Some(
            "CREATE RULE r AS \
             MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o.blocked = true, e.blocked = false \
             YIELD KEY e.uid AS uid \
             QUERY r RETURN uid",
        ),
        cypher: None,
    },
    Arm {
        label: "plain Cypher, same MATCH",
        locy: None,
        cypher: Some(
            "MATCH (o:Entity)-[s:OWNS]->(e:Entity) \
             WHERE o.blocked = true AND e.blocked = false \
             RETURN e.uid AS uid, sum(s.pct) AS agg",
        ),
    },
];

/// One arm at one scale.
struct Measurement {
    seconds: f64,
    rows: usize,
    iterations: usize,
}

/// Build the graph for one scale and return an open handle plus the real edge
/// count (self-loops are dropped, so it is slightly below the target).
async fn load(
    root: &str,
    n_edges: usize,
    reopen: bool,
) -> Result<(Uni, usize), Box<dyn std::error::Error>> {
    let db = Uni::open(root.to_string()).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (uid STRING, name STRING, blocked BOOL)")
        .await?;
    tx.execute("CREATE EDGE TYPE OWNS (pct FLOAT) FROM Entity TO Entity")
        .await?;
    tx.execute("CREATE INDEX idx_uid FOR (e:Entity) ON (e.uid)")
        .await?;
    tx.commit().await?;

    let n_nodes = (n_edges / 2).max(8);
    let mut rng = Rng(7);
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..n_nodes)
        .map(|i| {
            // ~13% blocked, close to the real register's designated ratio. The
            // predicate `o.blocked = true, e.blocked = false` is what makes the
            // derived set a fraction of the edge set rather than all of it.
            let blocked = rng.unit() < 0.13;
            HashMap::from([
                ("uid".to_string(), Value::String(format!("e{i}"))),
                (
                    "name".to_string(),
                    Value::String(format!("Entity Number {i}")),
                ),
                ("blocked".to_string(), Value::Bool(blocked)),
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("Entity", vertices).await?;

    let mut edges = Vec::with_capacity(n_edges);
    for _ in 0..n_edges {
        let a = rng.below(n_nodes);
        let b = rng.below(n_nodes);
        if a != b {
            let pct = 5.0 + rng.unit() * 95.0;
            edges.push(uni_bulk::EdgeData::new(
                vids[a],
                vids[b],
                HashMap::from([("pct".to_string(), Value::Float(pct))]),
            ));
        }
    }
    let n_real = edges.len();
    bulk.insert_edges("OWNS", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;

    // Reopening is a *lever*, not a fixture detail, which is why it is
    // switchable rather than hardcoded.
    //
    // `BulkWriter` rows are absent from `VidLabelsIndex` on the inserting
    // handle (#269), which escalates a batched vertex-property read to a scan
    // of every declared label (#264). Reopening rebuilds that index, and this
    // probe originally did it unconditionally to keep that fan-out off the
    // measurement. But the reporter's script queries the same handle that did
    // the loading, so it runs in the *un*-reopened state — and removing a
    // confound the defect depends on would silently delete the thing being
    // measured rather than isolate it. `LOCY_PROBE_REOPEN=0` restores the
    // reporter's arrangement so the two states can be compared instead of
    // assumed equivalent.
    if !reopen {
        return Ok((db, n_real));
    }
    drop(db);
    let db = Uni::open(root.to_string()).build().await?;
    Ok((db, n_real))
}

async fn run_arm(
    db: &Uni,
    arm: &Arm,
    budget: Duration,
) -> Result<Measurement, Box<dyn std::error::Error>> {
    if let Some(program) = arm.locy {
        // A generous ceiling: an arm that trips the deadline returns
        // `LocyIncomplete` rather than a silent partial, so a timeout shows up
        // as an error here and not as a fast, wrong row count.
        let config = uni_db::locy::LocyConfig {
            max_iterations: 1_000,
            timeout: budget,
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
        let (inner, _) = result.into_parts();
        let rows = inner.rows().map(|r| r.len()).unwrap_or(0);
        return Ok(Measurement {
            seconds,
            rows,
            iterations: inner.stats.total_iterations,
        });
    }
    let query = arm.cypher.expect("an arm is either locy or cypher");
    let started = Instant::now();
    let r = db.session().query(query).await?;
    Ok(Measurement {
        seconds: started.elapsed().as_secs_f64(),
        rows: r.rows().len(),
        iterations: 0,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    // Default on, so the measurement is clean; `LOCY_PROBE_REOPEN=0` puts the
    // fixture back into the reporter's state. See `load`.
    let reopen = std::env::var("LOCY_PROBE_REOPEN").as_deref() != Ok("0");
    println!(
        "reopen after bulk load: {}\n",
        if reopen {
            "yes"
        } else {
            "no (reporter's arrangement)"
        }
    );

    // `[arm][scale]`, so the exponent is read down a column.
    let mut grid: Vec<Vec<Measurement>> = Vec::new();
    for _ in ARMS {
        grid.push(Vec::new());
    }
    let mut edge_counts: Vec<usize> = Vec::new();

    for &target in SCALES {
        let dir = tempfile::tempdir()?;
        let root = dir.path().join("g").to_string_lossy().into_owned();
        let (db, n_edges) = load(&root, target, reopen).await?;
        edge_counts.push(n_edges);
        println!("=== {n_edges} OWNS edges / {} vertices ===", target / 2);
        println!(
            "  {:<32} {:>10} {:>8} {:>7}",
            "arm", "time", "rows", "iters"
        );
        for (i, arm) in ARMS.iter().enumerate() {
            let m = run_arm(&db, arm, Duration::from_secs(600)).await?;
            println!(
                "  {:<32} {:>9.3}s {:>8} {:>7}",
                arm.label, m.seconds, m.rows, m.iterations
            );
            grid[i].push(m);
        }
        println!();
    }

    // ---- the arms must be answering the same question ----------------------
    //
    // Two arms doing different amounts of work would make the comparison
    // meaningless, which is the failure mode a bare timing table hides. The
    // FOLD arms (0, 1, 2) all group the same edges by the same node under the
    // same threshold, so their row counts must agree; likewise the no-FOLD
    // pair (3, 4).
    for (scale, &n) in edge_counts.iter().enumerate() {
        let fold = [
            grid[0][scale].rows,
            grid[1][scale].rows,
            grid[2][scale].rows,
        ];
        assert!(
            fold.iter().all(|&r| r == fold[0]),
            "at {n} edges the FOLD arms returned {fold:?} rows; they are not comparable"
        );
        assert_eq!(
            grid[3][scale].rows, grid[4][scale].rows,
            "at {n} edges the no-FOLD arms are not comparable"
        );
    }

    // ---- exponent ----------------------------------------------------------
    println!("scaling exponent, log(t2/t1) / log(n2/n1) — ~1 linear, ~2 quadratic\n");
    println!("  {:<32} {:>28}", "arm", "exponent per doubling");
    for (i, arm) in ARMS.iter().enumerate() {
        let mut cells = String::new();
        for s in 1..edge_counts.len() {
            let (t1, t2) = (grid[i][s - 1].seconds, grid[i][s].seconds);
            let (n1, n2) = (edge_counts[s - 1] as f64, edge_counts[s] as f64);
            // A floor on t1: at these scales the fast arms run in single-digit
            // milliseconds, where timer noise would dominate the ratio and
            // manufacture an exponent out of nothing.
            let e = if t1 < 0.002 {
                f64::NAN
            } else {
                (t2 / t1).ln() / (n2 / n1).ln()
            };
            cells.push_str(&format!("{e:>13.2}"));
        }
        println!("  {:<32} {cells}", arm.label);
    }

    // ---- verdict -----------------------------------------------------------
    let last = edge_counts.len() - 1;
    let node = &grid[0][last];
    let scalar = &grid[1][last];
    let cypher = &grid[5][last];
    println!(
        "\n  at {} edges: KEY e is {:.0}x KEY e.uid, and {:.0}x plain Cypher",
        edge_counts[last],
        node.seconds / scalar.seconds.max(1e-9),
        node.seconds / cypher.seconds.max(1e-9),
    );

    // The discriminator. `compute_delta_legacy` rebuilds its seen-set per call,
    // so it is quadratic only if the call count grows with the data. If the
    // iteration count is flat while the exponent is ~2, the calls are not
    // per-iteration and the report's mechanism is incomplete as stated.
    let iters: Vec<usize> = (0..edge_counts.len())
        .map(|s| grid[0][s].iterations)
        .collect();
    let flat = iters.windows(2).all(|w| w[0] == w[1]);
    println!(
        "  KEY e iterations across scales: {iters:?} — {}",
        if flat {
            "FLAT, so a per-iteration rebuild cannot explain a quadratic curve"
        } else {
            "GROWING with scale, consistent with a per-iteration rebuild"
        }
    );
    Ok(())
}
