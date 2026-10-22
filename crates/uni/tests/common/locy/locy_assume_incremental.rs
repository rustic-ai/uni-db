// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A4: `ASSUME` and `ABDUCE` re-derive only the strata their mutation can
//! affect, and reuse the rest.
//!
//! Before, every hypothetical re-evaluation re-derived every stratum, so an
//! `ASSUME` that added one isolated node cost a second run of the whole
//! program (`rows_scanned` 1.97x the plain program at two scales). The
//! criterion is in `uni_locy::incremental`.
//!
//! Each answer is checked against the same program run fresh on the mutated
//! graph (the mutation applied in a transaction that is then rolled back), and
//! the work against counters: strata evaluated / reused, and `rows_scanned`
//! at two scales, so a regression to full re-evaluation fails.

// Rust guideline compliant

use std::collections::BTreeSet;

use anyhow::Result;
use uni_common::Value;
use uni_db::Uni;

type FactRow = std::collections::HashMap<String, Value>;
use uni_db::locy::{CommandResult, LocyConfig};

/// `KNOWS` is the large relation; `Company` is small and unrelated to it.
async fn seeded(n_people: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:Person {{idx: i}})",
        n_people - 1
    ))
    .await?;
    tx.execute(&format!(
        "MATCH (a:Person), (b:Person) WHERE b.idx = a.idx + 1 AND a.idx < {} \
         CREATE (a)-[:KNOWS]->(b)",
        n_people - 1
    ))
    .await?;
    tx.execute(
        "CREATE (:Company {name: 'acme', size: 20}), (:Company {name: 'tiny', size: 2}), \
                (:Company {name: 'mid', size: 12})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// Three strata: `knows` (reads KNOWS, large), `big` (reads Company), and
/// `big_named` (references `big`).
const PROGRAM: &str = "\
CREATE RULE knows AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b\n\
CREATE RULE big AS MATCH (c:Company) WHERE c.size > 10 YIELD KEY c\n\
CREATE RULE big_named AS MATCH (c:Company) WHERE c IS big YIELD KEY c, c.name AS name\n";

fn strings(rows: &[FactRow], col: &str) -> BTreeSet<String> {
    rows.iter().map(|r| format!("{:?}", r.get(col))).collect()
}

/// The rows of the ASSUME at `idx`, whose body's only command is a QUERY.
fn assume_rows(result: &uni_db::locy::LocyResult, idx: usize) -> Vec<FactRow> {
    match &result.command_results[idx] {
        CommandResult::Assume { results, .. } => match results.as_slice() {
            [CommandResult::Query(rows)] => rows.clone(),
            other => panic!("expected the ASSUME body's one QUERY result, got {other:?}"),
        },
        other => panic!("expected an ASSUME result, got {other:?}"),
    }
}

/// The program's QUERY answer on the graph with `mutation` applied, in a
/// transaction that is rolled back.
async fn fresh_answer(
    db: &Uni,
    mutation: &str,
    query: &str,
    col: &str,
) -> Result<BTreeSet<String>> {
    let tx = db.session().tx().await?;
    tx.execute(mutation).await?;
    let result = tx.locy(&format!("{PROGRAM}{query}")).await?;
    tx.rollback();
    let rows = match &result.command_results[0] {
        CommandResult::Query(rows) => rows.clone(),
        other => panic!("expected a QUERY result, got {other:?}"),
    };
    Ok(strings(&rows, col))
}

#[tokio::test]
async fn unaffected_strata_are_reused_and_the_answer_matches_a_fresh_run() -> Result<()> {
    let db = seeded(50).await?;
    let mutation = "CREATE (:Company {name: 'new', size: 50})";
    let query = "QUERY big_named RETURN name";
    let result = db
        .session()
        .locy(&format!(
            "{PROGRAM}ASSUME {{ {mutation} }} THEN {{ {query} }}"
        ))
        .await?;
    let got = strings(&assume_rows(&result, 0), "name");
    assert_eq!(got, fresh_answer(&db, mutation, query, "name").await?);
    assert!(got.contains("Some(String(\"new\"))"), "{got:?}");
    let stats = result.stats();
    // `knows` is reused; `big` reads Company and `big_named` references it.
    assert_eq!(stats.hypothetical_strata_reused, 1, "{stats:?}");
    assert_eq!(stats.hypothetical_strata_evaluated, 2, "{stats:?}");
    Ok(())
}

#[tokio::test]
async fn an_affected_lower_stratum_is_recomputed() -> Result<()> {
    let db = seeded(20).await?;
    // A new KNOWS edge from the last person back to the first.
    let mutation = "MATCH (a:Person {idx: 19}), (b:Person {idx: 0}) CREATE (a)-[:KNOWS]->(b)";
    let query = "QUERY knows RETURN a.idx AS src";
    let result = db
        .session()
        .locy(&format!(
            "{PROGRAM}ASSUME {{ {mutation} }} THEN {{ {query} }}"
        ))
        .await?;
    let got = strings(&assume_rows(&result, 0), "src");
    assert_eq!(got, fresh_answer(&db, mutation, query, "src").await?);
    assert!(got.contains("Some(Int(19))"), "{got:?}");
    let stats = result.stats();
    // `knows` is recomputed; the two Company strata are reused.
    assert_eq!(stats.hypothetical_strata_evaluated, 1, "{stats:?}");
    assert_eq!(stats.hypothetical_strata_reused, 2, "{stats:?}");
    Ok(())
}

#[tokio::test]
async fn a_property_change_recomputes_only_rules_reading_it() -> Result<()> {
    let db = seeded(20).await?;
    let mutation = "MATCH (c:Company {name: 'tiny'}) SET c.size = 99";
    let query = "QUERY big_named RETURN name";
    let result = db
        .session()
        .locy(&format!(
            "{PROGRAM}ASSUME {{ {mutation} }} THEN {{ {query} }}"
        ))
        .await?;
    let got = strings(&assume_rows(&result, 0), "name");
    assert_eq!(got, fresh_answer(&db, mutation, query, "name").await?);
    assert!(got.contains("Some(String(\"tiny\"))"), "{got:?}");
    // `big` reads `size`; `big_named` references `big`; `knows` reads neither.
    assert_eq!(result.stats().hypothetical_strata_reused, 1);

    // A property no rule reads changes nothing: every stratum is reused, and
    // the hydrated entity still shows the hypothetical value.
    let result = db
        .session()
        .locy(&format!(
            "{PROGRAM}ASSUME {{ MATCH (c:Company {{name: 'acme'}}) SET c.motto = 'hi' }} \
             THEN {{ QUERY big RETURN c.motto AS motto }}"
        ))
        .await?;
    let got = strings(&assume_rows(&result, 0), "motto");
    assert!(got.contains("Some(String(\"hi\"))"), "{got:?}");
    assert_eq!(result.stats().hypothetical_strata_reused, 3);
    assert_eq!(result.stats().hypothetical_strata_evaluated, 0);
    Ok(())
}

/// A DERIVE before the ASSUME changes the state the program's facts were
/// derived in, so they must not be reused.
///
/// Top-level commands run in sequence: the ASSUME reads the facts derived
/// again after the DERIVE (`sequenced_strata_*`), and reuses *those* for the
/// stratum its own mutation cannot affect. What must never be reused is the
/// pre-DERIVE state: that would answer with the 4 forward edges only.
#[tokio::test]
async fn a_write_before_the_assume_disables_reuse() -> Result<()> {
    let db = seeded(5).await?;
    let result = db
        .session()
        .locy(
            "CREATE RULE knows AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b\n\
             CREATE RULE back AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (b)-[:KNOWS]->(a)\n\
             DERIVE back\n\
             ASSUME { CREATE (:Company {name: 'x', size: 1}) } THEN { QUERY knows RETURN a.idx AS src }",
        )
        .await?;
    let rows = assume_rows(&result, 1);
    // 4 forward edges plus the 4 derived reverse ones.
    assert_eq!(rows.len(), 8, "{rows:?}");
    let stats = result.stats();
    // The DERIVE's write was folded in before the ASSUME read the facts:
    // both strata (`knows` reads KNOWS; `back` is a DERIVE rule) derived again.
    assert_eq!(stats.sequenced_strata_evaluated, 2, "{stats:?}");
    assert_eq!(stats.sequenced_strata_reused, 0, "{stats:?}");
    // The ASSUME's `CREATE (:Company)` cannot affect `knows`, so it reuses
    // the post-DERIVE facts for it; only `back` is derived.
    assert_eq!(stats.hypothetical_strata_reused, 1, "{stats:?}");
    assert_eq!(stats.hypothetical_strata_evaluated, 1, "{stats:?}");
    Ok(())
}

#[tokio::test]
async fn nested_assume_reuses_the_outer_hypothetical_state() -> Result<()> {
    let db = seeded(10).await?;
    let result = db
        .session()
        .locy(&format!(
            "{PROGRAM}ASSUME {{ CREATE (:Company {{name: 'outer', size: 30}}) }} THEN {{ \
               ASSUME {{ MATCH (a:Person {{idx: 9}}), (b:Person {{idx: 0}}) CREATE (a)-[:KNOWS]->(b) }} \
               THEN {{ QUERY big_named RETURN name }} \
               QUERY big_named RETURN name }}"
        ))
        .await?;
    // The outer ASSUME returns one result per body command: the nested
    // ASSUME's (its own QUERY's rows), then the outer QUERY's. Both read a
    // state with the outer `Company` in it; the nested ASSUME's KNOWS edge
    // does not change `big_named`, so both answer acme, mid and outer.
    let want: BTreeSet<String> = ["acme", "mid", "outer"]
        .iter()
        .map(|n| format!("{:?}", Some(Value::String((*n).to_string()))))
        .collect();
    let (nested, outer) = match &result.command_results[..] {
        [CommandResult::Assume { results, .. }] => match results.as_slice() {
            [
                CommandResult::Assume { results: inner, .. },
                CommandResult::Query(outer),
            ] => match inner.as_slice() {
                [CommandResult::Query(nested)] => (nested, outer),
                other => panic!("expected the nested body's one QUERY, got {other:?}"),
            },
            other => panic!("expected the nested ASSUME, then the QUERY, got {other:?}"),
        },
        other => panic!("expected one ASSUME result, got {other:?}"),
    };
    assert_eq!(strings(nested, "name"), want, "nested ASSUME's QUERY");
    assert_eq!(nested.len(), 3, "{nested:?}");
    assert_eq!(strings(outer, "name"), want, "outer QUERY");
    assert_eq!(outer.len(), 3, "{outer:?}");
    let stats = result.stats();
    // Outer: knows reused, big + big_named derived. Inner (KNOWS edge): knows
    // derived, big + big_named reused from the outer state.
    assert_eq!(stats.hypothetical_strata_reused, 3, "{stats:?}");
    assert_eq!(stats.hypothetical_strata_evaluated, 3, "{stats:?}");
    Ok(())
}

#[tokio::test]
async fn abduce_candidates_reuse_unaffected_strata() -> Result<()> {
    let db = seeded(10).await?;
    let result = db
        .session()
        .locy(&format!("{PROGRAM}ABDUCE NOT knows WHERE a.idx = 3"))
        .await?;
    let abduction = match &result.command_results[0] {
        CommandResult::Abduce(a) => a.clone(),
        other => panic!("expected ABDUCE, got {other:?}"),
    };
    assert!(
        abduction.modifications.iter().any(|m| m.validated),
        "{abduction:?}"
    );
    let stats = result.stats();
    let candidates = abduction.modifications.len();
    // Every candidate re-evaluates the three strata, deriving or reusing each.
    assert_eq!(
        stats.hypothetical_strata_reused + stats.hypothetical_strata_evaluated,
        3 * candidates,
        "{stats:?}"
    );
    // A `SET a.idx` candidate changes no rule input (the ABDUCE WHERE reads
    // `idx` from the re-hydrated facts), so all three strata are reused; the
    // KNOWS removal recomputes `knows` and reuses the Company strata.
    let removals = abduction
        .modifications
        .iter()
        .filter(|m| format!("{:?}", m.modification).starts_with("RemoveEdge"))
        .count();
    assert!(removals >= 1, "{abduction:?}");
    assert_eq!(stats.hypothetical_strata_evaluated, removals, "{stats:?}");
    Ok(())
}

/// The hypothetical state's facts must be complete: a re-evaluation cut short
/// by the iteration limit is an error, not a short answer.
#[tokio::test]
async fn an_incomplete_hypothetical_evaluation_is_an_error() -> Result<()> {
    let db = seeded(3).await?;
    let program = "\
        CREATE RULE reach AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b\n\
        CREATE RULE reach AS MATCH (a:Person)-[:KNOWS]->(m:Person) WHERE m IS reach TO b YIELD KEY a, KEY b\n\
        ASSUME { MATCH (a:Person {idx: 2}) \
                 CREATE (a)-[:KNOWS]->(:Person {idx: 3})-[:KNOWS]->(:Person {idx: 4})-[:KNOWS]->(:Person {idx: 5})-[:KNOWS]->(:Person {idx: 6})-[:KNOWS]->(:Person {idx: 7}) } \
        THEN { QUERY reach RETURN a.idx AS src }";
    let config = LocyConfig {
        max_iterations: 4,
        ..Default::default()
    };
    // The base chain of 3 converges within the limit; the hypothetical chain
    // of 8 does not.
    let err = db
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await
        .expect_err("a hypothetical run that hit the iteration limit must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.to_lowercase().contains("incomplete") || msg.to_lowercase().contains("iteration"),
        "{msg}"
    );
    Ok(())
}

/// Counter guard at two scales: an ASSUME that cannot affect the large
/// stratum must not rescan it. Measured as the scans the ASSUME adds over the
/// plain program, which must not grow with the large relation.
#[tokio::test]
async fn assume_cost_does_not_scale_with_unaffected_strata() -> Result<()> {
    let mut extra = Vec::new();
    for n in [2_000usize, 8_000] {
        let db = seeded(n).await?;
        // Settle the storage tiers before measuring. Seeding crosses the
        // auto-flush threshold at n=8000 (and the interval trigger at either
        // scale under load), and while that background flush is in flight a
        // scan reads the rows from both the pending L0 and the Lance version
        // already written: `rows_scanned` doubles for whichever arm lands in
        // the window, which made the extra-scan figure depend on timing. The
        // barrier drains it, so both arms read the same, quiescent tiers.
        db.flush().await?;
        // The first evaluation on a fresh database also pays one-off warm-up
        // scans; measure both arms after it.
        db.session().locy(PROGRAM).await?;
        let plain = db
            .session()
            .locy(&format!("{PROGRAM}QUERY big_named RETURN name"))
            .await?;
        let hypothetical = db
            .session()
            .locy(&format!(
                "{PROGRAM}ASSUME {{ CREATE (:Company {{name: 'new', size: 50}}) }} \
                 THEN {{ QUERY big_named RETURN name }}"
            ))
            .await?;
        let p = plain.metrics().rows_scanned;
        let h = hypothetical.metrics().rows_scanned;
        eprintln!(
            "[A4] n={n}: rows_scanned plain={p} assume={h} (l0/storage plain={}/{} assume={}/{}); \
             strata evaluated={} reused={} facts={}",
            plain.metrics().l0_reads,
            plain.metrics().storage_reads,
            hypothetical.metrics().l0_reads,
            hypothetical.metrics().storage_reads,
            hypothetical.stats().hypothetical_strata_evaluated,
            hypothetical.stats().hypothetical_strata_reused,
            hypothetical.stats().hypothetical_facts_derived,
        );
        assert!(
            p as usize >= n,
            "the plain run scans the KNOWS relation: {p}"
        );
        assert_eq!(hypothetical.stats().hypothetical_strata_reused, 1);
        extra.push(h.saturating_sub(p));
    }
    // Full re-evaluation adds about `p` (thousands, growing 4x); reuse adds a
    // few Company rows at both scales.
    assert!(
        extra.iter().all(|e| *e < 100),
        "an ASSUME on Company rescanned the KNOWS stratum: extra rows_scanned {extra:?}"
    );
    Ok(())
}
