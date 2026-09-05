//! Does an equality lookup on an indexed column consult its index at SF1?
//!
//! Every LDBC complex read starts from one — `Person {id: $personId}`,
//! `Tag {name: $tagName}` — and all fourteen report `scalar_idx=0`. That is
//! either "the planner does not use the index" or "the counter does not see it",
//! and a point lookup separates them: if a bare equality on an indexed column
//! also reports zero, and takes scan-like time, the index is not being used.
use std::time::{Duration, Instant};
use uni_db::{Uni, UniConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Uni::open(std::env::var("LDBC_DB")?)
        .config(UniConfig {
            query_timeout: Duration::from_secs(600),
            ..Default::default()
        })
        .build()
        .await?;
    for (label, q) in [
        // The shape LDBC uses: an inline property map in the pattern.
        (
            "inline map on indexed id",
            "MATCH (p:Person {id: 2199023262543}) RETURN p.firstName",
        ),
        // The same predicate written as a WHERE.
        (
            "WHERE on indexed id",
            "MATCH (p:Person) WHERE p.id = 2199023262543 RETURN p.firstName",
        ),
        (
            "WHERE on indexed name",
            "MATCH (t:Tag) WHERE t.name = 'Augustine_of_Hippo' RETURN t.name",
        ),
        (
            "WHERE IN on indexed id",
            "MATCH (p:Person) WHERE p.id IN [2199023262543] RETURN p.firstName",
        ),
        // Controls.
        (
            "scan control: id+0 = X",
            "MATCH (p:Person) WHERE p.id + 0 = 2199023262543 RETURN p.firstName",
        ),
        (
            "unindexed column equality",
            "MATCH (p:Person) WHERE p.browserUsed = 'Chrome' RETURN count(p)",
        ),
    ] {
        let t = Instant::now();
        let r = db.session().query(q).await?;
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let ex = db
            .session()
            .query_with(q)
            .explain()
            .await
            .map(|e| format!("{:?}", e.index_usage))
            .unwrap_or_else(|e| format!("explain failed: {e}"));
        let m = r.metrics();
        println!(
            "{label:<30} rows={:<3} {ms:>9.1} ms  scalar_idx={} cmp={} scans_reported={}",
            r.rows().len(),
            m.index_scans,
            m.index_comparisons,
            m.scans_reported
        );
        println!("      EXPLAIN index_usage: {}", &ex[..ex.len().min(240)]);
    }
    Ok(())
}
