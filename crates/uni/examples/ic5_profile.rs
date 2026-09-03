//! Times `invoke_cypher_udf`'s phases on LDBC IC5's `WHERE friend IN friends`.
//!
//! #245 bounded ~226 s of IC5 to "inside `predicate.evaluate`, outside the
//! `_cypher_in` closure and outside the vid read" — by subtraction, because the
//! per-argument timer that should have located it read zero. This measures
//! `invoke_cypher_udf`'s own total alongside each phase, so the subtraction is
//! replaced by a direct reading and a zero can be told apart from an
//! instrument that never fired.
//!
//!     LDBC_DB=$HOME/uni-bench-tmp/sf1 cargo run --release -p uni-db \
//!         --example ic5_profile

use std::time::Duration;

use uni_db::{Uni, UniConfig};

const IC5: &str = r#"
MATCH (person:Person { id: 2199023262543 })-[:KNOWS*1..2]-(friend)
WHERE NOT person = friend
WITH DISTINCT friend
MATCH (friend)<-[membership:HAS_MEMBER]-(forum)
WHERE membership.joinDate > 1262566914233
WITH forum, collect(friend) AS friends
OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
WHERE friend IN friends
WITH forum, count(post) AS postCount
RETURN forum.title AS forumName, postCount
ORDER BY postCount DESC, forum.id ASC
LIMIT 20
"#;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Uni::open(std::env::var("LDBC_DB")?)
        .config(UniConfig {
            query_timeout: Duration::from_secs(3600),
            ..Default::default()
        })
        .build()
        .await?;

    let (result, profile) = db.session().query_with(IC5).profile().await?;
    println!(
        "rows={} total={:.1} ms",
        result.rows().len(),
        profile.total_time_ms
    );

    let mut stats = profile.runtime_stats.clone();
    stats.sort_by(|a, b| {
        b.time_ms
            .partial_cmp(&a.time_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    println!("\n{:<40} {:>12} {:>14}", "operator", "ms", "rows");
    for s in stats.iter().take(6) {
        println!(
            "{:<40} {:>12.1} {:>14}",
            s.operator, s.time_ms, s.actual_rows
        );
    }
    Ok(())
}
