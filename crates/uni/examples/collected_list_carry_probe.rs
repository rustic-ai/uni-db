//! Measures what a *live* collected list costs when it is carried across a
//! fan-out — the shape LDBC IC3, IC10 and IC12 share, and the one #198's
//! chunking does not touch because none of those queries contains an `UNWIND`.
//!
//! `WITH collect(x) AS xs` produces one list, but every operator above copies
//! its input columns forward once per output row. So a list of `L` elements
//! read by a predicate above a traversal that emits `R` rows is materialised
//! `R × L` times. #184 removed the case where the list is *dead* at an
//! `UNWIND`; here it is live — the predicate reads it — so nothing may drop it.
//!
//! The probe holds one variable fixed and scales the other, against a control
//! that runs the identical traversal without the list. If peak allocation grows
//! with the product while the control stays flat, the replication is real; if
//! it tracks the control, this hypothesis is dead and the cost is elsewhere.
//!
//!     cargo run --release -p uni-db --example collected_list_carry_probe

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use uni_db::Uni;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Tracks live bytes and their high-water mark.
///
/// Peak allocation is the quantity in question and RSS cannot report it: it is
/// a process high-water mark that never falls, so consecutive measurements in
/// one process are not comparable. Counting at the allocator gives a figure per
/// query that can go back down.
struct Counting;

// SAFETY: every method forwards to `System` with the same layout it was given
// and returns its pointer unchanged; the counters are plain atomics and do not
// allocate, so no reentrancy is introduced.
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

/// Peak live bytes during `f`, measured from the current level.
async fn peak_of<F, T>(f: F) -> anyhow::Result<(T, f64)>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let out = f.await?;
    let peak = PEAK.load(Ordering::Relaxed);
    Ok((
        out,
        (peak.saturating_sub(before)) as f64 / (1024.0 * 1024.0),
    ))
}

/// `rows` persons in a KNOWS chain, each located in one of `cities` cities.
async fn fixture(rows: usize, cities: usize) -> anyhow::Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (idx INT)").await?;
    tx.execute("CREATE LABEL City (idx INT, name STRING)")
        .await?;
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P").await?;
    tx.execute("CREATE EDGE TYPE LIVES FROM P TO City").await?;

    for chunk in (0..=rows).collect::<Vec<_>>().chunks(500) {
        let stmt = chunk
            .iter()
            .map(|i| format!("(:P {{idx:{i}}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {stmt}")).await?;
    }
    for chunk in (0..cities).collect::<Vec<_>>().chunks(500) {
        let stmt = chunk
            .iter()
            .map(|i| format!("(:City {{idx:{i}, name:'c{i}'}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {stmt}")).await?;
    }
    tx.execute("MATCH (a:P), (b:P) WHERE b.idx = a.idx + 1 CREATE (a)-[:KNOWS]->(b)")
        .await?;
    // Every person lives somewhere, so the traversal emits one row per KNOWS
    // edge regardless of how many cities there are.
    tx.execute(&format!(
        "MATCH (b:P), (c:City) WHERE c.idx = b.idx % {cities} CREATE (b)-[:LIVES]->(c)"
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

// IC3's shape: collect entities, then read the list in a predicate above a
// traversal. The list is live, so no pruning may drop it.
const CARRIED: &str = "MATCH (c:City) WITH collect(c) AS cities \
     MATCH (a:P)-[:KNOWS]->(b:P)-[:LIVES]->(city:City) \
     WHERE city IN cities \
     RETURN count(*) AS n";

// The identical traversal with no list in scope.
const CONTROL: &str = "MATCH (a:P)-[:KNOWS]->(b:P)-[:LIVES]->(city:City) RETURN count(*) AS n";

// The list is in scope across the same traversal but nobody reads it. This
// separates carrying the column from evaluating the predicate over it: if this
// costs what CARRIED costs, the price is the copy and a projection could avoid
// it; if it costs what CONTROL costs, the price is the predicate.
const UNREAD: &str = "MATCH (c:City) WITH collect(c) AS cities \
     MATCH (a:P)-[:KNOWS]->(b:P)-[:LIVES]->(city:City) \
     RETURN count(*) AS n";

async fn count(db: &Uni, q: &str) -> anyhow::Result<i64> {
    let r = db.session().query(q).await?;
    match r.rows().first().map(|row| row.values()[0].clone()) {
        Some(uni_db::Value::Int(n)) => Ok(n),
        other => anyhow::bail!("expected a count, got {other:?}"),
    }
}

async fn measure(rows: usize, cities: usize) -> anyhow::Result<(i64, f64, f64, f64)> {
    let db = fixture(rows, cities).await?;
    let (control, mb_c) = peak_of(count(&db, CONTROL)).await?;
    let (unread, mb_u) = peak_of(count(&db, UNREAD)).await?;
    let (carried, mb_l) = peak_of(count(&db, CARRIED)).await?;
    // All three must answer the same, or the peaks are not comparable.
    anyhow::ensure!(
        control == carried && control == unread && carried == rows as i64,
        "expected {rows} rows from all three, got control={control} unread={unread} carried={carried}"
    );
    Ok((carried, mb_c, mb_u, mb_l))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("\n### Fixed fan-out (2000 rows), scaling the collected list\n");
    println!(
        "{:>8} {:>10} {:>14} {:>14} {:>14} {:>9}",
        "cities", "rows", "control MiB", "unread MiB", "carried MiB", "ratio"
    );
    for &cities in &[50usize, 100, 200, 400] {
        let (rows, mb_c, mb_u, mb_l) = measure(2000, cities).await?;
        println!(
            "{cities:>8} {rows:>10} {mb_c:>14.1} {mb_u:>14.1} {mb_l:>14.1} {:>8.1}x",
            mb_l / mb_c.max(0.001)
        );
    }

    println!("\n### Fixed list (200 cities), scaling the fan-out\n");
    println!(
        "{:>8} {:>10} {:>14} {:>14} {:>14} {:>9}",
        "cities", "rows", "control MiB", "unread MiB", "carried MiB", "ratio"
    );
    for &rows in &[500usize, 1000, 2000, 4000] {
        let (n, mb_c, mb_u, mb_l) = measure(rows, 200).await?;
        println!(
            "{:>8} {n:>10} {mb_c:>14.1} {mb_u:>14.1} {mb_l:>14.1} {:>8.1}x",
            200,
            mb_l / mb_c.max(0.001)
        );
    }

    println!(
        "\n`carried` growing with rows × cities while `control` stays flat means \
         the list is replicated onto every fan-out row.\n\
         `unread` equal to `carried` means the price is the column copy, not the \
         predicate — a list nobody reads costs the same as one that is read.\n"
    );
    Ok(())
}
