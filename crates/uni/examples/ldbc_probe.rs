//! Ad-hoc query probe against a persisted LDBC graph. Reads one Cypher query per
//! line from stdin so query shapes can be bisected without a rebuild.
//! Kept as a general instrument: it takes arbitrary Cypher on stdin against a
//! persisted LDBC graph, so a query shape can be bisected without a rebuild.
//! (The one-off probes for defects now covered by tests were deleted; see
//! `docs/testing/single-shape-coverage-2026-08-27.md` for the discipline.)
use std::io::BufRead;
use uni_db::Uni;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let secs: u64 = std::env::var("PROBE_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    // `PROBE_BATCH_SIZE` holds the edge set fixed while varying the morsel
    // size: a per-batch cost scales with it, a per-row cost does not.
    let batch_size: usize = std::env::var("PROBE_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);
    eprintln!("[probe] batch_size={batch_size} query_timeout={secs}s");
    let config = uni_db::UniConfig {
        query_timeout: std::time::Duration::from_secs(secs),
        batch_size,
        ..Default::default()
    };
    let db = Uni::open_existing(std::env::var("LDBC_DB")?)
        .config(config)
        .build()
        .await?;
    for line in std::io::stdin().lock().lines() {
        let q = line?;
        let q = q.trim();
        if q.is_empty() || q.starts_with('#') {
            continue;
        }
        // Elapsed is printed per query: a bisection ladder without per-step
        // timing cannot attribute a cost to a step, only observe that it ran.
        let t = std::time::Instant::now();
        let outcome = db.session().query(q).await;
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        match outcome {
            Ok(r) => println!(
                "rows={:<8} ms={ms:>10.1} first={:?}\n  {q}",
                r.rows().len(),
                r.rows().first().map(|x| x.values().to_vec())
            ),
            Err(e) => println!("ERROR ms={ms:>10.1} {e}\n  {q}"),
        }
    }
    Ok(())
}
