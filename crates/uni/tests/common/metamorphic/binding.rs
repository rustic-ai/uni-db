// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Binding guard: a query's rows cannot depend on how its text names things.
//!
//! Issue #300 was a Locy defect in which a name resolved by schema lookup
//! instead of by its binder captured an unrelated column, so the answer
//! depended on conjunct order and on variable names. Cypher has sites of the
//! same shape (bound-target detection, correlated-variable checks, the
//! `ORDER BY`-alias resolver), and probes of them were correct; this module
//! keeps them that way. For each generated case and each query variant:
//!
//! - **permute conjuncts:** `bag(q) == bag(permute_conjuncts(q, π))`;
//! - **alpha-rename:** `bag(q) == bag(alpha_rename(q, ρ))` once the reference
//!   bag's column labels are mapped through `ρ`.
//!
//! `ρ` is drawn from [`RENAMINGS`]: an `a`↔`b` swap, fresh names, aliases named
//! after property keys (including the `ORDER BY` key `name`), and variables
//! named after property keys. The variants include two ordered-and-limited
//! queries whose bags are deterministic, so a sort key resolved to the wrong
//! column changes the rows instead of only their order.
//!
//! **Anti-vacuity.** A permutation that moves no conjunct, or a reference that
//! returns no rows, proves nothing; both are counted and held to floors.

// Rust guideline compliant

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use proptest::prelude::*;
use uni_cypher::ast::Query;
use uni_db::Uni;

use super::{drive, run_bag, smoke_cases, soak_cases};
use crate::diff::{CanonRow, RowBag, bag_eq};
use crate::querygen::render::render;
use crate::querygen::transform::{
    RENAMINGS, alpha_rename, permute_conjuncts, permutes_any, renaming,
};
use crate::querygen::{Case, Partition, arb_case};

/// Minimum fraction of cases in which the permutation moves a conjunct.
///
/// Measured at 64 cases: see the rate the run prints. A permutation of
/// conjuncts needs a `WHERE` of at least two, which the generator emits when
/// it draws a base filter or a compound predicate.
const MIN_PERMUTED_RATE: f64 = 0.25;

/// Minimum fraction of query variants whose reference returns rows.
const MIN_NON_EMPTY_RATE: f64 = 0.40;

/// One generated case plus the rewrites applied to it.
#[derive(Clone, Debug)]
pub struct BindingCase {
    case: Case,
    perm: Vec<usize>,
    renaming: usize,
}

/// Cases with a random conjunct permutation and a renaming from [`RENAMINGS`].
fn arb_binding_case() -> impl Strategy<Value = BindingCase> {
    (
        arb_case(),
        Just((0..6).collect::<Vec<usize>>()).prop_shuffle(),
        0..RENAMINGS.len(),
    )
        .prop_map(|(case, perm, renaming)| BindingCase {
            case,
            perm,
            renaming,
        })
}

/// What a run saw, for the anti-vacuity floors.
#[derive(Default)]
struct Tally {
    cases: AtomicU64,
    permuted: AtomicU64,
    variants: AtomicU64,
    non_empty: AtomicU64,
}

/// The query variants every case is checked through.
fn variants(case: &Case) -> Vec<Query> {
    let mut out = vec![
        case.base_query(),
        case.partition_query(Partition::True),
        case.partition_query(Partition::False),
        case.partition_query(Partition::Null),
        case.norec_optimized(),
        case.norec_unoptimized(),
        case.ordered_limited_query(3),
        case.conjunctive_query(),
    ];
    out.extend(case.alias_ordered_limited_query(3));
    out
}

/// `bag` with every column label mapped through `map`, re-sorted canonically.
fn rename_bag(bag: &RowBag, map: &BTreeMap<String, String>) -> RowBag {
    let renamed: Vec<String> = bag
        .columns
        .iter()
        .map(|c| map.get(c).cloned().unwrap_or_else(|| c.clone()))
        .collect();
    let mut order: Vec<usize> = (0..renamed.len()).collect();
    order.sort_by(|&x, &y| renamed[x].cmp(&renamed[y]));
    RowBag {
        columns: order.iter().map(|&i| renamed[i].clone()).collect(),
        counts: bag
            .counts
            .iter()
            .map(|(row, &n)| {
                (
                    CanonRow(order.iter().map(|&i| row.0[i].clone()).collect()),
                    n,
                )
            })
            .collect(),
        total: bag.total,
    }
}

/// Checks both rewrites of every variant of one case.
///
/// # Errors
///
/// Returns the rendered queries and the bag difference on a disagreement, or
/// any query error.
async fn check_binding(db: &Uni, bc: &BindingCase, tally: &Tally) -> anyhow::Result<()> {
    let map = renaming(bc.renaming);
    let variants = variants(&bc.case);
    let case_number = tally.cases.fetch_add(1, Ordering::Relaxed) + 1;
    if variants.iter().any(|q| permutes_any(q, &bc.perm)) {
        tally.permuted.fetch_add(1, Ordering::Relaxed);
    }
    for q in &variants {
        let reference = run_bag(db, q).await?;
        tally.variants.fetch_add(1, Ordering::Relaxed);
        if reference.total > 0 {
            tally.non_empty.fetch_add(1, Ordering::Relaxed);
        }

        let permuted = permute_conjuncts(q, &bc.perm);
        if let Err(diff) = bag_eq(&reference, &run_bag(db, &permuted).await?) {
            anyhow::bail!(
                "permuting conjuncts changed the rows.\n  reference: {}\n  permuted:  {}\n{diff}",
                render(q),
                render(&permuted),
            );
        }

        let renamed = alpha_rename(q, &map);
        if let Err(diff) = bag_eq(&rename_bag(&reference, &map), &run_bag(db, &renamed).await?) {
            anyhow::bail!(
                "renaming changed the rows.\n  reference: {}\n  renamed:   {}\n{diff}",
                render(q),
                render(&renamed),
            );
        }
    }
    // Leave completed-case checkpoints in nextest's captured failure output,
    // so a timeout reports how much of the workload actually finished.
    if case_number.is_multiple_of(1_000) {
        eprintln!("[metamorphic:binding] completed_cases={case_number}");
    }
    Ok(())
}

/// Drives the guard over `cases` cases and holds the run to its floors.
fn drive_binding(cases: u32) {
    let tally = Arc::new(Tally::default());
    let shared = Arc::clone(&tally);
    drive(cases, arb_binding_case(), move |db, bc| {
        let tally = Arc::clone(&shared);
        Box::pin(async move { check_binding(db, bc, &tally).await })
    });
    let n = tally.cases.load(Ordering::Relaxed);
    let permuted = tally.permuted.load(Ordering::Relaxed);
    let variants = tally.variants.load(Ordering::Relaxed);
    let non_empty = tally.non_empty.load(Ordering::Relaxed);
    eprintln!(
        "[metamorphic:binding] cases={n} permuted={permuted} variants={variants} non_empty={non_empty}"
    );
    assert!(n > 0, "no case ran");
    assert!(
        permuted as f64 >= MIN_PERMUTED_RATE * n as f64,
        "only {permuted} of {n} cases moved a conjunct (floor {:.0}%)",
        MIN_PERMUTED_RATE * 100.0
    );
    assert!(
        non_empty as f64 >= MIN_NON_EMPTY_RATE * variants as f64,
        "only {non_empty} of {variants} references returned rows (floor {:.0}%)",
        MIN_NON_EMPTY_RATE * 100.0
    );
}

/// PR smoke gate for the binding guard.
#[test]
fn binding_guard_smoke() {
    drive_binding(smoke_cases());
}

/// Nightly soak for the binding guard; volume from `METAMORPHIC_CASES`.
#[test]
#[ignore = "soak: high-volume nightly run; volume set by METAMORPHIC_CASES"]
fn binding_guard_soak() {
    drive_binding(soak_cases());
}

#[cfg(test)]
mod targeted {
    use super::*;
    use crate::diff::bag;
    use crate::metamorphic::seed::build_seed;

    /// A sort key read from the wrong column changes the limited rows, so the
    /// ordered variants can tell an alias from a property of the same name.
    #[tokio::test]
    async fn ordered_limit_tells_alias_from_property() -> anyhow::Result<()> {
        let db = build_seed().await?;
        let s = db.session();
        let by_property = bag(&s
            .query("MATCH (a:Person) RETURN a.age AS name ORDER BY a.name ASC LIMIT 3")
            .await?);
        let by_alias = bag(&s
            .query("MATCH (a:Person) RETURN a.age AS name ORDER BY name ASC, a.name ASC LIMIT 3")
            .await?);
        assert!(
            bag_eq(&by_property, &by_alias).is_err(),
            "the two sort keys must keep different rows, else the variant is blind"
        );
        Ok(())
    }

    /// Relabelling a bag follows its columns: the renamed reference equals the
    /// renamed query's bag.
    #[tokio::test]
    async fn rename_bag_follows_the_columns() -> anyhow::Result<()> {
        let db = build_seed().await?;
        let s = db.session();
        let reference = bag(&s
            .query("MATCH (a:Person) WHERE a.age > 20 RETURN a.name AS c0, a.age AS c1")
            .await?);
        let renamed = bag(&s
            .query("MATCH (a:Person) WHERE a.age > 20 RETURN a.name AS z, a.age AS a")
            .await?);
        let map = BTreeMap::from([
            ("c0".to_owned(), "z".to_owned()),
            ("c1".to_owned(), "a".to_owned()),
        ]);
        assert!(bag_eq(&rename_bag(&reference, &map), &renamed).is_ok());
        assert!(bag_eq(&reference, &renamed).is_err());
        Ok(())
    }
}
