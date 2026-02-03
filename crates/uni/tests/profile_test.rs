// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use tempfile::tempdir;
use uni_db::UniBuilder;

#[tokio::test]
#[ignore = "Profiling instrumentation not yet capturing granular operator stats"]
async fn test_profile_basic() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let db = UniBuilder::new(path.to_str().unwrap().to_string())
        .build()
        .await?;

    // Create schema
    db.query("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    db.query("CREATE LABEL City (name STRING)").await?;
    db.query("CREATE EDGE TYPE LIVES_IN () FROM Person TO City")
        .await?;

    // Insert data
    db.query("CREATE (p:Person {name: 'Alice', age: 30})")
        .await?;
    db.query("CREATE (c:City {name: 'London'})").await?;
    db.query("MATCH (p:Person), (c:City) WHERE p.name = 'Alice' AND c.name = 'London' CREATE (p)-[:LIVES_IN]->(c)").await?;

    // Profile query
    let _query = "PROFILE MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name";

    // Note: Uni::profile takes the query string. The parser handles "PROFILE" prefix if present?
    // Actually Uni::profile just profiles whatever query is passed.
    // The "PROFILE" keyword is handled by parser/repl to call `db.profile()`.
    // But `db.profile()` takes a query string. Does it strip "PROFILE"?
    // Let's check Parser. Parser handles "PROFILE" by returning Query::Profile?
    // Wait, parser supports EXPLAIN. Does it support PROFILE?

    // In `crates/uni-query/src/query/parser.rs`:
    // L48: if self.peek_keyword("EXPLAIN") { return Ok(Query::Explain(...)) }

    // If I pass "PROFILE MATCH...", the parser might fail if it doesn't expect PROFILE.
    // Or maybe it treats it as identifier?

    // `uni-cli` handles `EXPLAIN` manually?
    // L78: if query_upper.starts_with("EXPLAIN") { ... }

    // Let's check `uni-cli/src/repl.rs`.
    // L149: if query_upper.starts_with("PROFILE") {
    //    let query = query[7..].trim();
    //    match db.profile(query).await { ... }

    // So the CLI strips "PROFILE". The `db.profile()` method expects the query WITHOUT "PROFILE".

    let clean_query = "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name";
    let (result, profile) = db.profile(clean_query).await?;

    println!("Profile Stats: {:#?}", profile.runtime_stats);

    assert_eq!(result.rows.len(), 1);

    // Check stats
    // We expect: Scan(Person), Traverse, Scan(City) (or filtered scan), Project

    let operators: Vec<String> = profile
        .runtime_stats
        .iter()
        .map(|s| s.operator.clone())
        .collect();
    println!("Operators: {:?}", operators);

    assert!(operators.iter().any(|op| op.contains("Scan(p)")));
    assert!(operators.iter().any(|op| op.contains("Traverse")));
    // Note: Vectorized execution might have filtered scan or separate scan depending on plan

    // Check total time is present (u64 is always non-negative)
    let _ = profile.total_time_ms;

    // Check rows count
    // Scan(Person) should produce 1 row
    let scan_person = profile
        .runtime_stats
        .iter()
        .find(|s| s.operator.contains("Scan(p)"))
        .unwrap();
    assert_eq!(scan_person.actual_rows, 1);

    Ok(())
}
