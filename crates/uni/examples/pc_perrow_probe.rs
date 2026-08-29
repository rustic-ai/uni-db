//! Measures the per-row pattern-comprehension fallback and shows that
//! `query_timeout` cannot preempt it.
//!
//! A comprehension whose pattern variables are all fresh cannot anchor, so it
//! compiles to `PatternComprehensionSubqueryExpr`: one sub-plan execution on a
//! scoped thread per outer row (expr_compiler.rs). The anchored form of the
//! same question takes the vectorized operator. This probe times both at
//! several fixture sizes, then runs the fallback under a 250 ms timeout and
//! reports how long the query actually held the session.
//!
//!     cargo run --release -p uni-db --example pc_perrow_probe

use std::time::{Duration, Instant};

use uni_db::Uni;

const SIZES: &[usize] = &[50, 100, 200, 400];

async fn build_chain(n: usize) -> anyhow::Result<uni_db::Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (idx INT)").await?;
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P").await?;
    for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
        let stmt = chunk
            .iter()
            .map(|i| format!("(:P {{idx:{i}}})"))
            .collect::<Vec<_>>()
            .join(", ");
        tx.execute(&format!("CREATE {stmt}")).await?;
    }
    tx.execute("MATCH (a:P), (b:P) WHERE b.idx = a.idx + 1 CREATE (a)-[:KNOWS]->(b)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

// Fresh pattern variables (a, b) + a correlated reference to the outer row:
// analyze_pattern cannot anchor, so this is the per-row subquery fallback.
const FALLBACK: &str = "MATCH (n:P) \
     RETURN n.idx AS i, size([(a:P)-[:KNOWS]->(b:P) WHERE a.idx > n.idx | 1]) AS s";

// Same question anchored on the outer variable: vectorized path.
const ANCHORED: &str = "MATCH (n:P) RETURN n.idx AS i, size([(n)-[:KNOWS]->(b) | 1]) AS s";

// The IC14 shape: reduce over relationships(p) with the correlated
// comprehension inside — the fallback fires per path element per row.
const IC14_SHAPE: &str = "MATCH p = (n:P {idx:0})-[:KNOWS*1..2]->(m:P) \
     RETURN reduce(acc = 0, r IN relationships(p) | \
         acc + size([(a:P)-[:KNOWS]->(b:P) WHERE a.idx > n.idx | 1])) AS w";

async fn timed(db: &Uni, q: &str) -> anyhow::Result<(usize, f64)> {
    let t = Instant::now();
    let r = db.session().query(q).await?;
    Ok((r.rows().len(), t.elapsed().as_secs_f64() * 1e3))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("\n### Scaling: anchored (vectorized) vs fresh-variable (per-row fallback)\n");
    println!(
        "{:>6} {:>14} {:>14} {:>8} {:>12}",
        "N", "anchored ms", "fallback ms", "ratio", "per-row µs"
    );
    for &n in SIZES {
        let db = build_chain(n).await?;
        let (rows_a, ms_a) = timed(&db, ANCHORED).await?;
        let (rows_f, ms_f) = timed(&db, FALLBACK).await?;
        anyhow::ensure!(rows_a == n && rows_f == n, "row count mismatch");
        println!(
            "{:>6} {:>14.1} {:>14.1} {:>7.1}x {:>12.0}",
            n,
            ms_a,
            ms_f,
            ms_f / ms_a,
            ms_f * 1e3 / n as f64
        );
    }

    let db = build_chain(*SIZES.last().unwrap()).await?;
    println!("\n### IC14 shape (reduce over relationships(p), correlated inner pattern)\n");
    let (rows, ms) = timed(&db, IC14_SHAPE).await?;
    println!("    {rows} rows in {ms:.1} ms");

    // The overrun past the deadline scales with the data, because nothing can
    // preempt the per-row evaluation: the timeout is only observed after the
    // last row's sub-plan finishes.
    println!("\n### Timeout: the fallback under query_with().timeout(250ms)\n");
    for n in [400usize, 2000] {
        let db = build_chain(n).await?;
        let session = db.session();
        let t = Instant::now();
        let res = session
            .query_with(FALLBACK)
            .timeout(Duration::from_millis(250))
            .fetch_all()
            .await;
        let elapsed = t.elapsed().as_secs_f64() * 1e3;
        match res {
            Ok(r) => println!(
                "    N={n:<5} OK  {} rows after {elapsed:.1} ms (timeout was 250 ms — never fired)",
                r.rows().len()
            ),
            Err(e) => println!(
                "    N={n:<5} ERR after {elapsed:.1} ms, {:.0}x past the 250 ms deadline: {e}",
                elapsed / 250.0
            ),
        }
    }
    Ok(())
}
