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
    println!(
        "{arm:<34} {got:>10} {:>12.1} {:>12.0} {ms:>9.0}",
        peak / (1024.0 * 1024.0),
        peak / rows as f64
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
    ] {
        run(&session, arm, q, rows).await?;
    }

    Ok(())
}
