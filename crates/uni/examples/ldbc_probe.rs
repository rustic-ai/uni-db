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
    let config = uni_db::UniConfig {
        query_timeout: std::time::Duration::from_secs(secs),
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
        match db.session().query(q).await {
            Ok(r) => println!(
                "rows={:<4} first={:?}\n  {q}",
                r.rows().len(),
                r.rows().first().map(|x| x.values().to_vec())
            ),
            Err(e) => println!("ERROR {e}\n  {q}"),
        }
    }
    Ok(())
}
