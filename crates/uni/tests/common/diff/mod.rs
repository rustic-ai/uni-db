//! Order-insensitive multiset (bag) comparison of query results.
//!
//! The metamorphic oracles compare query results as **bags** of rows: row order
//! is ignored, and columns are canonicalized by sorted name so two results that
//! `RETURN` the same columns in a different order still compare equal. This is
//! the single dependency shared by the TLP and NoREC oracles.
//!
//! # Why a bag and not a set
//!
//! Cypher results are multisets — duplicate rows are significant (e.g.
//! `RETURN a.age` over rows with the same age). The TLP law
//! `bag(Q) == bag(Q WHERE p) ⊎ bag(Q WHERE NOT p) ⊎ bag(Q WHERE p IS NULL)`
//! only holds with multiset (`⊎`) union, so we count occurrences, never dedup.
//!
//! # Id stability
//!
//! [`Value::Node`]/`Edge`/`Path` carry ids that are stable only within one
//! database/process. Every metamorphic comparison runs all of its sub-queries
//! against the **same** freshly-built database, so those ids are identical
//! across the sub-queries and compare correctly. Never compare bags built from
//! different database instances.

use std::collections::HashMap;
use std::fmt;

use uni_query::{QueryResult, Value};

/// Maximum number of differing rows shown per side in a [`BagDiff`] report.
const MAX_SHOWN: usize = 20;

/// A single result row reduced to a column-order-independent key.
///
/// The values are reordered to follow the column names sorted lexicographically,
/// so the same logical row compares equal regardless of `RETURN` column order.
/// `Vec<Value>` is a valid hash-map key because [`Value`] implements `Eq` and
/// `Hash` with float normalization (`0.0 == -0.0`, `NaN == NaN`).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CanonRow(pub Vec<Value>);

/// A multiset of canonicalized rows plus the canonical (sorted) column schema.
#[derive(Clone, Debug, Default)]
pub struct RowBag {
    /// Column names, sorted lexicographically (the canonical schema).
    pub columns: Vec<String>,
    /// Occurrence count per distinct canonicalized row.
    pub counts: HashMap<CanonRow, usize>,
    /// Total number of rows (sum of `counts` values).
    pub total: usize,
}

/// The symmetric difference between two unequal [`RowBag`]s.
///
/// Either the schemas differ ([`schema_mismatch`](BagDiff::schema_mismatch)), or
/// some rows occur more often on one side than the other. `only_left` holds rows
/// with a positive left-minus-right count delta; `only_right` the reverse.
#[derive(Debug)]
pub struct BagDiff {
    /// `Some((left_cols, right_cols))` when the canonical column schemas differ.
    pub schema_mismatch: Option<(Vec<String>, Vec<String>)>,
    /// Rows (with the surplus count) present more on the left than the right.
    pub only_left: Vec<(CanonRow, usize)>,
    /// Rows (with the surplus count) present more on the right than the left.
    pub only_right: Vec<(CanonRow, usize)>,
    /// Total left-side row count.
    pub left_total: usize,
    /// Total right-side row count.
    pub right_total: usize,
}

/// Builds the column permutation taking sorted-name order to physical indices.
///
/// `perm[k]` is the physical column index of the k-th column in sorted-name
/// order. A stable sort on `(name, physical_index)` keeps duplicate column names
/// deterministic.
fn sorted_permutation(columns: &[String]) -> (Vec<String>, Vec<usize>) {
    let mut indexed: Vec<(usize, &String)> = columns.iter().enumerate().collect();
    indexed.sort_by(|(ia, a), (ib, b)| a.cmp(b).then(ia.cmp(ib)));
    let sorted_names = indexed.iter().map(|(_, n)| (*n).clone()).collect();
    let perm = indexed.iter().map(|(i, _)| *i).collect();
    (sorted_names, perm)
}

/// Reduces a query result to a multiset of canonicalized rows.
///
/// Column order is canonicalized to sorted-name order; row order is discarded;
/// duplicate rows are counted.
pub fn bag(result: &QueryResult) -> RowBag {
    let (columns, perm) = sorted_permutation(result.columns());
    let mut counts: HashMap<CanonRow, usize> = HashMap::new();
    let mut total = 0;
    for row in result.rows() {
        let values = row.values();
        let canon = CanonRow(perm.iter().map(|&i| values[i].clone()).collect());
        *counts.entry(canon).or_insert(0) += 1;
        total += 1;
    }
    RowBag {
        columns,
        counts,
        total,
    }
}

/// Compares two bags as multisets, returning the symmetric difference on
/// mismatch.
///
/// An **empty** result reports empty columns (an engine behavior: a zero-row
/// `QueryResult` carries no schema), so schema equality is enforced only when
/// both sides are non-empty. An empty bag is the empty multiset, compatible with
/// any schema; a genuine row-count difference is still caught by the per-row
/// count comparison below.
///
/// # Errors
///
/// Returns [`BagDiff`] when both sides are non-empty with differing canonical
/// schemas, or when any row's occurrence count differs between the two bags.
pub fn bag_eq(a: &RowBag, b: &RowBag) -> Result<(), BagDiff> {
    if a.total > 0 && b.total > 0 && a.columns != b.columns {
        return Err(BagDiff {
            schema_mismatch: Some((a.columns.clone(), b.columns.clone())),
            only_left: Vec::new(),
            only_right: Vec::new(),
            left_total: a.total,
            right_total: b.total,
        });
    }
    let mut only_left = Vec::new();
    let mut only_right = Vec::new();
    for (row, &ca) in &a.counts {
        let cb = b.counts.get(row).copied().unwrap_or(0);
        if ca > cb {
            only_left.push((row.clone(), ca - cb));
        }
    }
    for (row, &cb) in &b.counts {
        let ca = a.counts.get(row).copied().unwrap_or(0);
        if cb > ca {
            only_right.push((row.clone(), cb - ca));
        }
    }
    if only_left.is_empty() && only_right.is_empty() {
        Ok(())
    } else {
        Err(BagDiff {
            schema_mismatch: None,
            only_left,
            only_right,
            left_total: a.total,
            right_total: b.total,
        })
    }
}

/// Checks that `sub` is a sub-multiset of `sup`: every row occurs in `sub` no
/// more often than in `sup`.
///
/// Like [`bag_eq`], schema equality is enforced only when both sides are
/// non-empty (an empty result reports empty columns). Used by the LIMIT
/// structural law (`bag(Q LIMIT n) ⊆ bag(Q)`).
///
/// # Errors
///
/// Returns a [`BagDiff`] whose `only_left` lists the rows where `sub` exceeds
/// `sup` (with the surplus count), or a schema mismatch when both are non-empty
/// with differing schemas.
pub fn bag_is_subset(sub: &RowBag, sup: &RowBag) -> Result<(), BagDiff> {
    if sub.total > 0 && sup.total > 0 && sub.columns != sup.columns {
        return Err(BagDiff {
            schema_mismatch: Some((sub.columns.clone(), sup.columns.clone())),
            only_left: Vec::new(),
            only_right: Vec::new(),
            left_total: sub.total,
            right_total: sup.total,
        });
    }
    let mut only_left = Vec::new();
    for (row, &cs) in &sub.counts {
        let cu = sup.counts.get(row).copied().unwrap_or(0);
        if cs > cu {
            only_left.push((row.clone(), cs - cu));
        }
    }
    if only_left.is_empty() {
        Ok(())
    } else {
        Err(BagDiff {
            schema_mismatch: None,
            only_left,
            only_right: Vec::new(),
            left_total: sub.total,
            right_total: sup.total,
        })
    }
}

/// Multiset union (`⊎`) of several bags, summing occurrence counts.
///
/// Used to reunite TLP partitions. **Empty** parts are skipped: a zero-row
/// result reports empty columns, and the empty multiset contributes nothing and
/// imposes no schema. All non-empty parts must share the canonical schema.
///
/// # Panics
///
/// Panics if two non-empty parts have differing canonical column schemas — that
/// indicates a generator bug (the partitions should all `RETURN` the same
/// columns), not a data-level mismatch.
pub fn bag_union(parts: &[RowBag]) -> RowBag {
    let mut out: Option<RowBag> = None;
    for part in parts {
        // Skip the empty multiset: it adds no rows and carries no schema.
        if part.total == 0 {
            continue;
        }
        match &mut out {
            None => out = Some(part.clone()),
            Some(acc) => {
                assert_eq!(
                    acc.columns, part.columns,
                    "bag_union: non-empty parts have different schemas (generator bug)"
                );
                for (row, &c) in &part.counts {
                    *acc.counts.entry(row.clone()).or_insert(0) += c;
                }
                acc.total += part.total;
            }
        }
    }
    out.unwrap_or_default()
}

impl fmt::Display for BagDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some((left, right)) = &self.schema_mismatch {
            return write!(
                f,
                "bag schema mismatch: left columns {left:?} != right columns {right:?}"
            );
        }
        writeln!(
            f,
            "bag mismatch: left_total={}, right_total={}",
            self.left_total, self.right_total
        )?;
        let show = |f: &mut fmt::Formatter<'_>, label: &str, rows: &[(CanonRow, usize)]| {
            writeln!(f, "  {label} ({} distinct):", rows.len())?;
            for (row, delta) in rows.iter().take(MAX_SHOWN) {
                writeln!(f, "    x{delta} {:?}", row.0)?;
            }
            if rows.len() > MAX_SHOWN {
                writeln!(f, "    ... and {} more", rows.len() - MAX_SHOWN)?;
            }
            Ok(())
        };
        show(f, "only on left", &self.only_left)?;
        show(f, "only on right", &self.only_right)
    }
}

#[cfg(test)]
mod tests {
    use super::{bag, bag_eq, bag_union};
    use uni_db::{DataType, Uni};

    /// Builds an in-memory db with three `X` nodes of values 1, 1, 2 so bags
    /// exercise multiplicity (the `1` appears twice).
    async fn db_with_values() -> anyhow::Result<Uni> {
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("X")
            .property("v", DataType::Int)
            .done()
            .apply()
            .await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:X {v: 1})").await?;
        tx.execute("CREATE (:X {v: 1})").await?;
        tx.execute("CREATE (:X {v: 2})").await?;
        tx.commit().await?;
        Ok(db)
    }

    /// `bag_is_subset` must reject what it is supposed to reject.
    ///
    /// Added when CERT (`metamorphic::dqp::cert`) took a dependency on it and it
    /// turned out to have no test at all, while its sibling `bag_eq` had one.
    /// An oracle is only as good as its comparator: a `bag_is_subset` that
    /// returned `Ok` unconditionally would make every CERT case pass, and the
    /// narrowing-rate floor would not notice — that floor proves the *inputs*
    /// differ, not that the *check* works.
    #[tokio::test]
    async fn bag_is_subset_has_teeth() -> anyhow::Result<()> {
        use super::bag_is_subset;
        let db = db_with_values().await?;
        let session = db.session();

        let all = bag(&session.query("MATCH (n:X) RETURN n.v AS v").await?);
        let ones = bag(&session
            .query("MATCH (n:X) WHERE n.v = 1 RETURN n.v AS v")
            .await?);

        assert!(bag_is_subset(&all, &all).is_ok(), "a bag contains itself");
        assert!(
            bag_is_subset(&ones, &all).is_ok(),
            "{{1,1}} is a sub-bag of {{1,1,2}}"
        );
        assert!(
            bag_is_subset(&all, &ones).is_err(),
            "{{1,1,2}} is NOT a sub-bag of {{1,1}} — the 2 is unmatched"
        );

        // Multiplicity, not just set membership: {1,1} must not fit inside {1}.
        let one_only = bag(&session
            .query("MATCH (n:X) WHERE n.v = 1 RETURN n.v AS v LIMIT 1")
            .await?);
        assert!(
            bag_is_subset(&one_only, &ones).is_ok(),
            "{{1}} fits inside {{1,1}}"
        );
        assert!(
            bag_is_subset(&ones, &one_only).is_err(),
            "{{1,1}} must NOT fit inside {{1}} — subset is over multisets, and a \
             set-based check would wrongly accept this"
        );
        Ok(())
    }

    #[tokio::test]
    async fn bag_counts_multiplicity_and_is_reflexive() -> anyhow::Result<()> {
        let db = db_with_values().await?;
        let all = bag(&db.session().query("MATCH (n:X) RETURN n.v AS v").await?);
        assert_eq!(all.total, 3, "three rows");
        assert_eq!(all.counts.len(), 2, "two distinct values");
        assert!(bag_eq(&all, &all).is_ok(), "a bag equals itself");
        Ok(())
    }

    #[tokio::test]
    async fn bag_is_column_order_independent() -> anyhow::Result<()> {
        let db = db_with_values().await?;
        let session = db.session();
        let a = bag(&session
            .query("MATCH (n:X) RETURN n.v AS v, n.v AS w")
            .await?);
        let b = bag(&session
            .query("MATCH (n:X) RETURN n.v AS w, n.v AS v")
            .await?);
        assert!(
            bag_eq(&a, &b).is_ok(),
            "swapping RETURN column order must not change the bag"
        );
        Ok(())
    }

    #[tokio::test]
    async fn bag_union_reunites_a_partition() -> anyhow::Result<()> {
        let db = db_with_values().await?;
        let session = db.session();
        let q = "MATCH (n:X) RETURN n.v AS v";
        let all = bag(&session.query(q).await?);
        let lo = bag(&session
            .query("MATCH (n:X) WHERE n.v <= 1 RETURN n.v AS v")
            .await?);
        let hi = bag(&session
            .query("MATCH (n:X) WHERE n.v > 1 RETURN n.v AS v")
            .await?);
        let reunited = bag_union(&[lo, hi]);
        assert!(
            bag_eq(&all, &reunited).is_ok(),
            "partition by v<=1 / v>1 must reunite to the whole"
        );
        Ok(())
    }

    #[tokio::test]
    async fn empty_partition_is_schema_neutral() -> anyhow::Result<()> {
        // A zero-row result reports empty columns; reuniting it with non-empty
        // partitions must not trip a schema mismatch (regression for the TLP
        // all-false-predicate case).
        let db = db_with_values().await?;
        let session = db.session();
        let all = bag(&session.query("MATCH (n:X) RETURN n.v AS v").await?);
        let empty = bag(&session
            .query("MATCH (n:X) WHERE n.v < 0 RETURN n.v AS v")
            .await?);
        let lo = bag(&session
            .query("MATCH (n:X) WHERE n.v <= 1 RETURN n.v AS v")
            .await?);
        let hi = bag(&session
            .query("MATCH (n:X) WHERE n.v > 1 RETURN n.v AS v")
            .await?);
        assert_eq!(empty.total, 0, "the <0 partition is empty");
        let reunited = bag_union(&[empty, lo, hi]);
        assert!(
            bag_eq(&all, &reunited).is_ok(),
            "an empty partition must reunite cleanly"
        );
        Ok(())
    }

    #[tokio::test]
    async fn bag_eq_has_teeth() -> anyhow::Result<()> {
        let db = db_with_values().await?;
        let session = db.session();
        let all = bag(&session.query("MATCH (n:X) RETURN n.v AS v").await?);
        let partial = bag(&session
            .query("MATCH (n:X) WHERE n.v <= 1 RETURN n.v AS v")
            .await?);
        let diff = bag_eq(&all, &partial).expect_err("whole != strict subset");
        assert_eq!(diff.left_total, 3);
        assert_eq!(diff.right_total, 2);
        assert_eq!(diff.only_left.len(), 1, "the v=2 row is only on the left");
        Ok(())
    }
}
