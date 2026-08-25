//! Does a pristine fork agree with its parent on row *identity*?
//!
//! The DQP fork lever compares result **bags** between primary and a pristine
//! fork, and `querygen` generates queries that project a bare variable —
//! `Shape::projection_choices` (`querygen/mod.rs:168-177`) emits
//! `Expr::Variable(var)`, so `RETURN a` is a routine case. That yields a
//! `Value::Node` carrying an id, and `diff/mod.rs:14-21` warns in as many words
//! that bags carrying ids are only comparable within one database.
//!
//! The Tier-2 design calls the primary-vs-fork lever "identity-preserving",
//! reasoning that a fork inherits its parent's rows — and therefore their VIDs —
//! through the branch's `base_paths` chain. **That was an assumption.** If it is
//! wrong, every generated case projecting a bare variable diffs for a reason
//! that has nothing to do with a bug, and the lever is unusable as designed.
//!
//! These tests settle it. They are a prerequisite for the lever, not an
//! afterthought: if they fail, the fallback is to restrict DQP projections to
//! properties only.

use uni_db::Session;

use super::seed::{Tier, build_dqp_seed};

/// Reads `(id, name)` pairs for every `Person`, ordered by name so the two
/// sides are directly zippable.
async fn id_name_pairs(session: &Session) -> anyhow::Result<Vec<(i64, String)>> {
    let r = session
        .query("MATCH (p:Person) RETURN id(p) AS vid, p.name AS name ORDER BY p.name")
        .await?;
    r.rows()
        .iter()
        .map(|row| Ok((row.get::<i64>("vid")?, row.get::<String>("name")?)))
        .collect()
}

/// A pristine fork must report the **same** `id(n)` for an inherited row as its
/// parent does.
///
/// This is the precondition the whole Tier-2 lever rests on.
#[tokio::test(flavor = "multi_thread")]
async fn pristine_fork_preserves_vids_of_inherited_rows() -> anyhow::Result<()> {
    let db = build_dqp_seed(Tier::Tiny).await?;

    let primary = id_name_pairs(&db.session()).await?;
    assert!(!primary.is_empty(), "the fixture must have rows to compare");

    let fork = db.session().fork("dqp_identity").await?;
    let forked = id_name_pairs(&fork).await?;

    assert_eq!(
        primary.len(),
        forked.len(),
        "a pristine fork must see exactly the parent's rows"
    );
    for (a, b) in primary.iter().zip(forked.iter()) {
        assert_eq!(
            a.1, b.1,
            "row ordering diverged, so the comparison below is meaningless"
        );
        assert_eq!(
            a.0, b.0,
            "VID for '{}' differs across the fork boundary (primary {}, fork {}) \
             — the fork lever cannot compare bags containing node values, and \
             DQP projections must be restricted to properties",
            a.1, a.0, b.0
        );
    }
    Ok(())
}

/// The stronger claim the lever actually depends on: a bare `RETURN p` — the
/// projection `querygen` emits — compares equal across the fork boundary.
///
/// Identical VIDs are necessary but not sufficient; the `Value::Node` also
/// carries labels and properties, and any of them rendering differently on the
/// branch path would diff just as loudly.
#[tokio::test(flavor = "multi_thread")]
async fn bare_variable_projection_compares_equal_across_a_pristine_fork() -> anyhow::Result<()> {
    let db = build_dqp_seed(Tier::Tiny).await?;
    let q = "MATCH (p:Person) WHERE p.age = 30 RETURN p";

    let primary_result = db.session().query(q).await?;
    let fork = db.session().fork("dqp_identity_bag").await?;
    let fork_result = fork.query(q).await?;

    // The witness first: if the fork did not actually read through its branch,
    // this test proves nothing about the branch path.
    assert!(
        fork_result.metrics().branch_scans > 0,
        "the fork side must have executed a branch scan, or this comparison \
         says nothing about the fork read path"
    );
    assert_eq!(
        primary_result.metrics().branch_scans,
        0,
        "the primary side must not have"
    );

    let a = crate::diff::bag(&primary_result);
    let b = crate::diff::bag(&fork_result);
    assert!(a.total > 0, "the probe must select some rows");
    crate::diff::bag_eq(&a, &b).map_err(|d| {
        anyhow::anyhow!(
            "a bare `RETURN p` differs between primary and a pristine fork, so \
             node values are not comparable across the fork boundary:\n{d}"
        )
    })?;
    Ok(())
}

/// Edge identity, which the `Shape::Edge` cases exercise via `RETURN a, b`.
#[tokio::test(flavor = "multi_thread")]
async fn edge_shape_projection_compares_equal_across_a_pristine_fork() -> anyhow::Result<()> {
    let db = build_dqp_seed(Tier::Tiny).await?;
    let q = "MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN a, b";

    let primary_result = db.session().query(q).await?;
    let fork = db.session().fork("dqp_identity_edge").await?;
    let fork_result = fork.query(q).await?;

    let a = crate::diff::bag(&primary_result);
    let b = crate::diff::bag(&fork_result);
    assert!(a.total > 0, "the edge probe must select some rows");
    crate::diff::bag_eq(&a, &b).map_err(|d| {
        anyhow::anyhow!("edge-shape projection differs across a pristine fork:\n{d}")
    })?;
    Ok(())
}
