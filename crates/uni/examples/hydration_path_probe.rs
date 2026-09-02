//! Reproduces the scan-vs-traversal property-hydration gap on a local fixture.
//!
//! Reading a column through a scan and reaching the same column through a
//! traversal cost wildly different amounts per row (#209): at LDBC SF1, 16 B/row
//! against 2140 B/row. The scan reads Arrow columns; the traversal routes every
//! target vid through `PropertyManager::get_batch_vertex_props*`, which scans
//! the same columns and then shreds the batch into a `HashMap<Vid, HashMap<
//! String, Value>>` per row.
//!
//! SF1 is an 8.7 GB fixture and a multi-minute run, which is a poor feedback
//! loop for a fix. This builds the same comparison in memory in seconds, so the
//! acceptance criterion can be checked directly:
//!
//! * a traversal reading one column should approach the scan's per-row cost;
//! * and it should scale with the number of properties read, rather than being
//!   flat — flat is the signature of a per-row allocation that ignores the
//!   request.
//!
//!     cargo run --release -p uni-db --example hydration_path_probe

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use uni_db::{Uni, Value};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

// SAFETY: every method forwards to `System` with the layout it was given and
// returns its pointer unchanged; the counters are atomics and do not allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            let live = LIVE.fetch_add(new_size, Ordering::Relaxed) + new_size;
            PEAK.fetch_max(live, Ordering::Relaxed);
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const N: usize = 60_000;

/// `N` targets carrying three properties, each reached from one source, so a
/// traversal emits exactly `N` rows and a scan sees exactly `N` rows.
async fn fixture(decoys: usize) -> anyhow::Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Src (idx INT)").await?;
    tx.execute("CREATE LABEL Tgt (idx INT, p1 INT, p2 INT, p3 STRING)")
        .await?;
    tx.execute("CREATE EDGE TYPE R FROM Src TO Tgt").await?;

    for chunk in (0..N).collect::<Vec<_>>().chunks(2_000) {
        let src = chunk
            .iter()
            .map(|i| format!("(:Src {{idx:{i}}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {src}")).await?;
        let tgt = chunk
            .iter()
            .map(|i| format!("(:Tgt {{idx:{i}, p1:{i}, p2:{i}, p3:'v{i}'}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {tgt}")).await?;
    }
    // Rows in the target table that no edge reaches. They cannot change the
    // answer, and they cannot change how many rows are hydrated -- so if
    // per-row cost moves with them, hydration is re-scanning the whole target
    // table rather than reading the rows it was asked for.
    for chunk in (N..N + decoys).collect::<Vec<_>>().chunks(2_000) {
        let tgt = chunk
            .iter()
            .map(|i| format!("(:Tgt {{idx:{i}, p1:{i}, p2:{i}, p3:'v{i}'}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {tgt}")).await?;
    }
    tx.execute(&format!(
        "MATCH (s:Src), (t:Tgt) WHERE t.idx = s.idx AND t.idx < {N} CREATE (s)-[:R]->(t)"
    ))
    .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

async fn run(session: &uni_db::Session, arm: &str, q: &str, rows: usize) -> anyhow::Result<()> {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let t = Instant::now();
    let out = session.query(q).await?;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    let peak = (PEAK.load(Ordering::Relaxed).saturating_sub(before)) as f64;
    let got = match &out.rows()[0].values()[0] {
        Value::Int(n) => *n,
        other => anyhow::bail!("expected a count, got {other:?}"),
    };
    // `scans_reported` is the denominator: `index_scans == 0` only means "no
    // index consulted" when a scan actually reported. Zero reported scans means
    // the counter never fired and the zero says nothing (`counters.rs:203`).
    let m = out.metrics();
    println!(
        "{arm:<34} {got:>10} {:>12.1} {:>12.0} {ms:>9.0} {:>5}/{:<4} {:>12}",
        peak / (1024.0 * 1024.0),
        peak / rows as f64,
        m.index_scans,
        m.scans_reported,
        m.index_comparisons
    );
    std::io::stdout().flush().ok();
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    for decoys in [0usize, 4 * N] {
        probe(decoys).await?;
    }
    Ok(())
}

async fn probe(decoys: usize) -> anyhow::Result<()> {
    let db = fixture(decoys).await?;
    // One session for every arm, and one untimed query through it first.
    // A fresh session per arm rebuilds the adjacency cache each time, and that
    // fixed cost then swamps the per-row difference the probe exists to show.
    let session = db.session();
    session
        .query("MATCH (s:Src)-[:R]->(t:Tgt) RETURN count(*) AS n")
        .await?;

    println!(
        "\n== {N} traversal rows, {decoys} unreachable rows in the target table\n\n\
         {:<34} {:>10} {:>12} {:>12} {:>9}",
        "arm", "n", "peak MiB", "bytes/row", "ms"
    );
    println!("{:>96} {:>12}", "idx/scans", "idx_compares");
    // Rows each arm actually produces: a scan sees every target row including
    // the decoys, a traversal only the reachable ones.
    let scanned = N + decoys;
    for (arm, q, rows) in [
        (
            "scan, no property",
            "MATCH (t:Tgt) RETURN count(*) AS n",
            scanned,
        ),
        (
            "scan, 1 property",
            "MATCH (t:Tgt) RETURN count(t.p1) AS n",
            scanned,
        ),
        (
            "scan, 3 properties",
            "MATCH (t:Tgt) RETURN count(t.p1) + count(t.p2) + count(t.p3) AS n",
            scanned,
        ),
        (
            "traverse, no property",
            "MATCH (s:Src)-[:R]->(t:Tgt) RETURN count(*) AS n",
            N,
        ),
        (
            "traverse, 1 property",
            "MATCH (s:Src)-[:R]->(t:Tgt) RETURN count(t.p1) AS n",
            N,
        ),
        (
            "traverse, 3 properties",
            "MATCH (s:Src)-[:R]->(t:Tgt) \
             RETURN count(t.p1) + count(t.p2) + count(t.p3) AS n",
            N,
        ),
        // Without `:Tgt` in the pattern there is no `hasLabel` target filter to
        // verify, which separates label verification from property hydration.
        (
            "traverse untyped, no property",
            "MATCH (s:Src)-[:R]->(t) RETURN count(*) AS n",
            N,
        ),
        (
            "traverse untyped, 1 property",
            "MATCH (s:Src)-[:R]->(t) RETURN count(t.p1) AS n",
            N,
        ),
        // Repeats. A one-time structure build (CSR, a vid->labels index, a
        // property cache) is paid by whichever arm runs first and is free
        // afterwards; a per-query cost repeats. This is what separates them.
        (
            "traverse untyped, no property #2",
            "MATCH (s:Src)-[:R]->(t) RETURN count(*) AS n",
            N,
        ),
        (
            "traverse untyped, 1 property #2",
            "MATCH (s:Src)-[:R]->(t) RETURN count(t.p1) AS n",
            N,
        ),
        (
            "traverse untyped, 1 property #3",
            "MATCH (s:Src)-[:R]->(t) RETURN count(t.p1) AS n",
            N,
        ),
        // Same edges, same adjacency, reading a property of the *source*
        // instead of the target. The CSR is identical; only which side is
        // hydrated differs.
        (
            "traverse, source property",
            "MATCH (s:Src)-[:R]->(t) RETURN count(s.idx) AS n",
            N,
        ),
    ] {
        run(&session, arm, q, rows).await?;
    }

    // Why is the typed arm (`(t:Tgt)`) ~7x the untyped one on the same graph,
    // same rows, same property? These separate the label *filter* from the
    // label *column* it is evaluated over.
    println!("\n-- typed-arm bisection\n");
    for (arm, q, rows) in [
        // The filter, via the pattern.
        (
            "y1 pattern label (t:Tgt)",
            "MATCH (s:Src)-[:R]->(t:Tgt) RETURN count(*) AS n",
            N,
        ),
        // The same predicate written explicitly, so it is a WHERE rather than
        // a pattern label.
        (
            "y2 WHERE 'Tgt' IN labels(t)",
            "MATCH (s:Src)-[:R]->(t) WHERE 'Tgt' IN labels(t) RETURN count(*) AS n",
            N,
        ),
        // The labels column with no filter over it at all: isolates building
        // the column from evaluating a predicate on it.
        (
            "y3 labels(t) read, no filter",
            "MATCH (s:Src)-[:R]->(t) RETURN count(labels(t)) AS n",
            N,
        ),
        // Control: no label anywhere.
        (
            "y4 no label at all",
            "MATCH (s:Src)-[:R]->(t) RETURN count(*) AS n",
            N,
        ),
        // The same label read reached by a scan instead of a traversal. If the
        // scan is cheap, label resolution is traversal-specific, exactly as
        // property hydration was.
        (
            "y5 scan, labels(t) read",
            "MATCH (t:Tgt) RETURN count(labels(t)) AS n",
            N + decoys,
        ),
        (
            "y6 scan, no labels",
            "MATCH (t:Tgt) RETURN count(*) AS n",
            N + decoys,
        ),
    ] {
        run(&session, arm, q, rows).await?;
    }

    // Step 0 of the vid-lookup plan: compaction reaches `optimize_indices`,
    // which re-covers a scalar index over fragments written after it was built.
    // If cost tracks *uncovered* fragments -- Lance answers those with a full
    // scan unioned into the indexed take -- these arms collapse. If they do
    // not, the coverage hypothesis is dead and nothing should be built on it.
    let t = Instant::now();
    db.compaction().compact("Tgt").await?;
    println!(
        "\n-- after compact(\"Tgt\") in {:.0} ms\n",
        t.elapsed().as_secs_f64() * 1e3
    );
    for (arm, q, rows) in [
        (
            "post-compact scan, 1 property",
            "MATCH (t:Tgt) RETURN count(t.p1) AS n",
            scanned,
        ),
        (
            "post-compact traverse, no prop",
            "MATCH (s:Src)-[:R]->(t) RETURN count(*) AS n",
            N,
        ),
        (
            "post-compact traverse, 1 prop",
            "MATCH (s:Src)-[:R]->(t) RETURN count(t.p1) AS n",
            N,
        ),
        (
            "post-compact traverse, 3 props",
            "MATCH (s:Src)-[:R]->(t) \
             RETURN count(t.p1) + count(t.p2) + count(t.p3) AS n",
            N,
        ),
    ] {
        run(&session, arm, q, rows).await?;
    }

    Ok(())
}
