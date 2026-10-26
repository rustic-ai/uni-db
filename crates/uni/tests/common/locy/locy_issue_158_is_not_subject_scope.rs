// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for <https://github.com/rustic-ai/uni-db/issues/158>
//!
//! `WHERE x IS NOT some_rule` silently does not filter — every row is returned,
//! with no error and no warning.
//!
//! ## The issue's stated trigger is wrong
//!
//! The report attributes the failure to `x` "not being a KEY column". Measured
//! behaviour (the matrix below) shows KEY-ness is irrelevant:
//!
//! | consuming rule's YIELD    | negated subject | filters? |
//! |---------------------------|-----------------|----------|
//! | `YIELD KEY b, a`          | `a`             | yes      |
//! | `YIELD KEY b, KEY a AS s` | `s`             | yes      |
//! | `YIELD KEY b, a AS s`     | `s`             | yes      |
//! | `YIELD KEY b, a AS s`     | `a`             | **no**   |
//! | `YIELD KEY b`             | `a`             | **no**   |
//!
//! A *non*-KEY column filtered correctly (row 1) and a *KEY* column failed once
//! aliased (row 4). The real predicate was purely name-based: did a column with
//! that exact name exist in the rule's **projected output**? All five rows now
//! filter.
//!
//! ## Root cause (FIXED)
//!
//! Two independent faults composed.
//!
//! 1. **Phase ordering.** The anti-join runs *after* `LocyProject`
//!    (`Body(yield + positive IS) -> IS NOT(anti-join) -> PROB -> post-fixpoint`),
//!    and `LocyProject` emits exactly the YIELD names. A variable bound in
//!    MATCH but not yielded — or yielded under an alias — no longer existed as
//!    a column by the time the negation was evaluated. The user writes `WHERE`
//!    as scoping over MATCH variables; it was implemented as scoping over
//!    projection outputs. `issue_158_positive_is_ref_is_unaffected` pins the
//!    asymmetry that gave this away: the *positive* `IS` form resolves the same
//!    subject correctly, because it runs inside the body plan.
//!
//! 2. **Fail-open degradation.** Given an unresolvable column, the anti-join
//!    emitted precisely the rows it was asked to exclude — silent, and in the
//!    security-relevant direction. The originating report describes a
//!    tax-exemption guard (`WHERE c IS NOT exempt_customer`) with no effect at
//!    all.
//!
//! Fault 2 was closed first (unresolvable subjects became a hard error). Fault
//! 1 is closed by projecting a hidden `{var}._vid` column for every negated
//! subject that is a MATCH-bound node variable, so resolution keys on node
//! identity rather than on whatever YIELD chose to call things — see
//! `LocyIsRef::negation_keys` and `strip_isnot_vid_columns`.
//!
//! Subjects that are *not* MATCH-bound node variables have no `._vid` column
//! (relationship variables expose `._eid`; scalar subjects have neither), so
//! they cannot key an anti-join that joins on node identity. Those are now
//! **rejected at compile time** by `LocyCompileError::IsNotSubjectNotANode`,
//! rather than reaching the runtime error Wave 0 introduced — pinned by
//! `issue_158_non_node_subject_is_rejected_rather_than_failing_open`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(issue_158)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// `a1` is flagged (it has an outgoing `E`); `a2` is not.
async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("n", DataType::String)
        .done()
        .label("B")
        .property("n", DataType::String)
        .done()
        .edge_type("E", &["A"], &["B"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:A {n: 'a1'})").await?;
    tx.execute("CREATE (:A {n: 'a2'})").await?;
    tx.execute("CREATE (:B {n: 'b1'})").await?;
    tx.execute("MATCH (a:A {n: 'a1'}), (b:B {n: 'b1'}) CREATE (a)-[:E]->(b)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

const FLAGGED: &str = "CREATE RULE flagged AS MATCH (a:A)-[:E]->(b:B) YIELD KEY a, KEY b";

/// Runs `probe` with the given YIELD clause and negated subject, returning the
/// sorted distinct `A.n` values that survived the filter.
///
/// `col` names the output column holding the `A` node.
async fn probe(db: &Uni, yield_clause: &str, subject: &str, col: &str) -> Result<Vec<String>> {
    let program = format!(
        "{FLAGGED}\n\
         CREATE RULE probe AS\n\
           MATCH (a:A), (b:B)\n\
           WHERE {subject} IS NOT flagged\n\
           {yield_clause}\n"
    );
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    let facts = result
        .derived_rule("probe")
        .await
        .unwrap()
        .unwrap_or(&empty);

    let mut names: Vec<String> = facts
        .iter()
        .filter_map(|row| match row.get(col) {
            Some(Value::Node(node)) => match node.properties.get("n") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    names.sort();
    names.dedup();
    Ok(names)
}

// ---------------------------------------------------------------------------
// Green: the paths that DO work. These pin the boundary of the bug — each one
// falsifies the "must be a KEY column" hypothesis from the issue report.
// ---------------------------------------------------------------------------

/// A **non-KEY** yielded column filters correctly, so KEY-ness is not the
/// discriminator. This is the single most important assertion in the file.
#[tokio::test]
async fn issue_158_non_key_column_with_own_name_does_filter() -> Result<()> {
    let db = setup().await?;
    let got = probe(&db, "YIELD KEY b, a", "a", "a").await?;
    assert_eq!(
        got,
        vec!["a2".to_string()],
        "a non-KEY column that keeps its own name must still filter; \
         KEY-ness is not what makes IS NOT work"
    );
    Ok(())
}

/// The mirror: a KEY column reached through its **alias** also filters, so the
/// resolution is by projected name in both directions.
#[tokio::test]
async fn issue_158_key_column_via_alias_does_filter() -> Result<()> {
    let db = setup().await?;
    let got = probe(&db, "YIELD KEY b, KEY a AS subj", "subj", "subj").await?;
    assert_eq!(got, vec!["a2".to_string()]);
    Ok(())
}

/// A non-KEY aliased column filters when the negation names the alias — the
/// documented workaround for the bug until the scope fix lands.
#[tokio::test]
async fn issue_158_aliased_column_filters_under_the_alias_name() -> Result<()> {
    let db = setup().await?;
    let got = probe(&db, "YIELD KEY b, a AS subj", "subj", "subj").await?;
    assert_eq!(got, vec!["a2".to_string()]);
    Ok(())
}

/// The **positive** `IS` form resolves a subject the negated form cannot,
/// because it is evaluated in the body plan before `LocyProject`. This
/// asymmetry is the direct evidence for the phase-ordering root cause.
#[tokio::test]
async fn issue_158_positive_is_ref_is_unaffected() -> Result<()> {
    let db = setup().await?;
    let program = format!(
        "{FLAGGED}\n\
         CREATE RULE probe AS\n\
           MATCH (a:A), (b:B)\n\
           WHERE a IS flagged\n\
           YIELD KEY b, a AS subj\n"
    );
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    let facts = result
        .derived_rule("probe")
        .await
        .unwrap()
        .unwrap_or(&empty);
    let names: Vec<String> = facts
        .iter()
        .filter_map(|row| match row.get("subj") {
            Some(Value::Node(node)) => match node.properties.get("n") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(
        names,
        vec!["a1".to_string()],
        "positive IS resolves the same subject that negated IS NOT cannot"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The fix: resolution by node identity, not by projected name.
//
// The planner now projects a hidden `{var}._vid` column for every negated
// subject that is a MATCH-bound node variable, and the anti-join keys on that
// (see `LocyIsRef::negation_keys`). Both programs below are semantically
// valid — `a` is bound in MATCH — and now filter correctly regardless of what
// YIELD named it.
//
// Wave 0 made these error rather than silently return every row; this is the
// second half, which makes them work.
// ---------------------------------------------------------------------------

/// The subject is bound in MATCH and yielded, but under an alias, so the bare
/// name is not a post-projection column. Resolution now goes through `a._vid`.
#[tokio::test]
async fn issue_158_aliased_subject_filters_correctly() -> Result<()> {
    let db = setup().await?;
    let got = probe(&db, "YIELD KEY b, a AS subj", "a", "subj").await?;
    assert_eq!(
        got,
        vec!["a2".to_string()],
        "`a` is MATCH-bound, so renaming it in YIELD must not disable the negation"
    );
    Ok(())
}

/// The subject is bound in MATCH but never yielded as a node column, so no
/// name could reach it. This is the exemption-guard case from the originating
/// report — the one that billed tax to exempt customers.
#[tokio::test]
async fn issue_158_unprojected_subject_filters_correctly() -> Result<()> {
    let db = setup().await?;
    let program = format!(
        "{FLAGGED}\n\
         CREATE RULE probe AS\n\
           MATCH (a:A), (b:B)\n\
           WHERE a IS NOT flagged\n\
           YIELD KEY b, a.n AS name\n"
    );
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    let facts = result
        .derived_rule("probe")
        .await
        .unwrap()
        .unwrap_or(&empty);
    let mut names: Vec<String> = facts
        .iter()
        .filter_map(|row| match row.get("name") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["a2".to_string()],
        "the subject need not be projected at all — it is resolved by node identity"
    );
    Ok(())
}

/// A subject that is **not** a MATCH-bound node variable has no `._vid` column,
/// so it cannot key an anti-join that joins on node identity — and must fail
/// loudly rather than silently returning every row.
///
/// Here `x` is bound only by the YIELD alias, never by the MATCH pattern.
///
/// The failure has moved phase twice. Originally it silently returned every
/// row; Wave 0 made it a runtime error; it is now rejected at **compile time**
/// (`LocyCompileError::IsNotSubjectNotANode`), so the program never evaluates
/// at all. The assertion is deliberately phase-agnostic — it asserts the error
/// identifies the negation, not where it came from — but the message check is
/// tightened here to confirm the compile-time path specifically, since a
/// silent regression to the runtime path would still contain `IS NOT`.
#[tokio::test]
async fn issue_158_non_node_subject_is_rejected_rather_than_failing_open() -> Result<()> {
    let db = setup().await?;
    let program = format!(
        "{FLAGGED}\n\
         CREATE RULE probe AS\n\
           MATCH (a:A), (b:B)\n\
           WHERE x IS NOT flagged\n\
           YIELD KEY b, a.n AS x\n"
    );
    let err = db
        .session()
        .locy(&program)
        .await
        .err()
        .expect("a non-node IS NOT subject must not silently pass every row");
    let msg = err.to_string();
    assert!(
        msg.contains("IS NOT"),
        "error must identify the failing negation; got: {msg}"
    );
    assert!(
        msg.contains("IS NOT subject 'x'"),
        "must be the compile-time rejection naming the subject, not the later \
         runtime resolution failure; got: {msg}"
    );
    Ok(())
}

/// The hidden `_vid` column must not survive into the derived relation.
///
/// If the strip is missed, `reconcile_schema` adopts the widened schema as the
/// rule's fact identity and `write_facts_to_registry` silently falls back to it
/// — corrupting cross-stratum IS-refs with no diagnostic. FOLD would mask this
/// (`FoldExec` emits only key + fold columns), so this asserts on a **non-FOLD**
/// rule.
#[tokio::test]
async fn issue_158_hidden_vid_column_does_not_leak_into_derived() -> Result<()> {
    let db = setup().await?;
    let program = format!(
        "{FLAGGED}\n\
         CREATE RULE probe AS\n\
           MATCH (a:A), (b:B)\n\
           WHERE a IS NOT flagged\n\
           YIELD KEY b, a AS subj\n"
    );
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    let facts = result
        .derived_rule("probe")
        .await
        .unwrap()
        .unwrap_or(&empty);
    assert!(
        !facts.is_empty(),
        "precondition: the rule must derive facts"
    );
    for row in facts {
        let leaked: Vec<&String> = row.keys().filter(|k| k.starts_with("__isnot")).collect();
        assert!(
            leaked.is_empty(),
            "hidden anti-join column leaked into derived facts: {leaked:?} \
             (columns: {:?})",
            row.keys().collect::<Vec<_>>()
        );
    }
    Ok(())
}
