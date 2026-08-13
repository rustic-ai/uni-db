//! Tiered fixture graphs for the DQP oracle.
//!
//! The existing metamorphic fixture (`metamorphic::seed`) is 12 `Person` + 4
//! `Company` + 10 `WORKS_AT`, never flushed. That is deliberate and correct for
//! TLP/NoREC, which only need three-valued logic to be reachable. It is useless
//! for DQP: at 26 rows Lance never engages an index, everything stays in L0, and
//! every execution path collapses to the same scan — so both sides of a
//! differential comparison exercise identical machinery and the oracle passes
//! vacuously. The repo already contains a live instance of that failure mode:
//! `fork_index_recall_bench.rs` reports recall@10 = 1.000 because at n=1000
//! Lance brute-forces and the index under test never runs.
//!
//! # Schema is fixed by the generator, not chosen here
//!
//! `querygen` hardcodes its shapes and property sets (`querygen/mod.rs:60-116`):
//! `Person { name (NOT NULL), age?, score?, city? }`,
//! `Company { name (NOT NULL), founded? }`, and `(:Person)-[:WORKS_AT]->(:Company)`.
//! This module reproduces exactly that schema so the generator works against it
//! unchanged.
//!
//! # Value domains are fixed by the generator too
//!
//! Less obvious, and load-bearing: `arb_literal` (`querygen/mod.rs:466-478`)
//! selects from **hardcoded literal sets** — `age ∈ {0,18,22,25,30,40,50,65,
//! 1999,2010}`, `score ∈ {0.0,0.2,0.5,0.7,0.9,1.0}`, `city ∈ {NYC,SF,LA,ZZ,p1}`.
//! A fixture populated with, say, uniformly random ages in `0..100` would make
//! almost every generated equality predicate match nothing, and the oracle would
//! compare empty bags against empty bags all day while reporting green.
//!
//! So the tiers scale by **repeating the small fixture's value domain over more
//! rows**, never by widening it. Predicate selectivity stays roughly constant as
//! the row count grows, which is exactly the property a differential oracle
//! wants: more data through the same filters, not different filters.
//!
//! # Tiers
//!
//! Sizes here are **hypotheses to be measured** by [`super::feasibility`], not
//! settings to be trusted. They exist to be refuted.

use std::collections::HashMap;

use uni_common::core::id::Vid;
use uni_db::{DataType, Uni, Value, unival};

/// Boundary value for `age`, mirroring `metamorphic::seed::AGE_BOUNDARY`.
///
/// Several Persons sit exactly on it so off-by-one partitioning (`<` vs `<=`)
/// has something to catch.
pub const AGE_BOUNDARY: i64 = 30;

/// `age` values drawn by the fixture — a subset of `arb_literal`'s Int set that
/// reads as a plausible age, so `=`, `<` and `>` predicates all have matches.
const AGE_DOMAIN: &[i64] = &[18, 22, 25, 30, 40, 50, 65];
/// `score` values drawn by the fixture — exactly `arb_literal`'s Float set.
const SCORE_DOMAIN: &[f64] = &[0.0, 0.2, 0.5, 0.7, 0.9, 1.0];
/// `city` values drawn by the fixture. `arb_literal` also emits `ZZ` and `p1`,
/// which deliberately match **no** row — always-false predicates are a partition
/// worth exercising.
const CITY_DOMAIN: &[&str] = &["NYC", "SF", "LA"];
/// `founded` values drawn by the fixture — `arb_literal`'s two year literals.
const FOUNDED_DOMAIN: &[i64] = &[1999, 2010];

/// NULL is injected on a fixed modulus rather than at random, so the NULL rate
/// is exact at every tier and the fixture self-test can assert it.
const AGE_NULL_EVERY: u64 = 6;
const SCORE_NULL_EVERY: u64 = 4;
const CITY_NULL_EVERY: u64 = 6;
const FOUNDED_NULL_EVERY: u64 = 4;
/// Every Nth Person gets no `WORKS_AT` edge, so relationship patterns drop them
/// (mirrors the two edge-less Persons in the small fixture).
const EDGELESS_EVERY: u64 = 7;

/// Vertices per `bulk_insert_vertices` call, following
/// `perf::test_100k_lookup` (10k vertices in 5k batches).
const VERTEX_BATCH: usize = 5_000;
/// Edges per `bulk_insert_edges` call, same source.
const EDGE_BATCH: usize = 10_000;

/// Fixture size class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// ~1k vertices — cheap to rebuild, so it carries generator *volume*.
    Tiny,
    /// ~10k vertices — the PR-gate fixture.
    Smoke,
    /// ~50k vertices — engages index paths, morsel batching and multi-fragment
    /// scans. Properties of the *data*, which a few hundred cases exercise as
    /// well as fifty thousand do.
    Large,
}

impl Tier {
    /// All tiers, smallest first — the measurement order.
    pub const ALL: [Tier; 3] = [Tier::Tiny, Tier::Smoke, Tier::Large];

    /// Short name used in report tables and test output.
    pub fn name(self) -> &'static str {
        match self {
            Tier::Tiny => "tiny",
            Tier::Smoke => "smoke",
            Tier::Large => "large",
        }
    }

    /// Number of `Person` rows.
    pub fn persons(self) -> usize {
        match self {
            Tier::Tiny => 1_000,
            Tier::Smoke => 10_000,
            Tier::Large => 50_000,
        }
    }

    /// Number of `Company` rows — kept at ~4% of Persons so the `Company` side
    /// of a join stays small and `WORKS_AT` fans in, as it does in reality.
    pub fn companies(self) -> usize {
        match self {
            Tier::Tiny => 40,
            Tier::Smoke => 400,
            Tier::Large => 2_000,
        }
    }

    /// Number of `WORKS_AT` edges (~4 per non-edgeless Person).
    pub fn edges(self) -> usize {
        match self {
            Tier::Tiny => 4_000,
            Tier::Smoke => 40_000,
            Tier::Large => 200_000,
        }
    }
}

/// Deterministic xorshift64*, seeded per fixture so a tier is byte-identical
/// across rebuilds.
///
/// Reproducibility is not decoration here: the Phase-4 stateful driver shrinks
/// failures by rebuilding both sides of a comparison from a recorded seed, which
/// only works if the fixture is a pure function of that seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn pick<T: Copy>(&mut self, domain: &[T]) -> T {
        domain[(self.next() % domain.len() as u64) as usize]
    }
}

/// Applies the `querygen`-mandated schema.
async fn apply_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int)
        .property_nullable("score", DataType::Float)
        .property_nullable("city", DataType::String)
        .done()
        .label("Company")
        .property("name", DataType::String)
        .property_nullable("founded", DataType::Int)
        .done()
        .edge_type("WORKS_AT", &["Person"], &["Company"])
        .apply()
        .await?;
    Ok(())
}

/// Builds one batch of `Person` property maps for indices `range`.
///
/// A nullable property is made NULL by **omitting the key**, matching how the
/// small fixture's `CREATE` statements leave properties unset.
fn person_batch(range: std::ops::Range<usize>, rng: &mut Rng) -> Vec<HashMap<String, Value>> {
    range
        .map(|i| {
            let mut p: HashMap<String, Value> = HashMap::new();
            p.insert("name".to_string(), unival!(format!("p{i}")));
            let n = i as u64;
            if n % AGE_NULL_EVERY != 0 {
                p.insert("age".to_string(), unival!(rng.pick(AGE_DOMAIN)));
            } else {
                rng.next(); // keep the stream aligned regardless of NULL choice
            }
            if n % SCORE_NULL_EVERY != 0 {
                p.insert("score".to_string(), unival!(rng.pick(SCORE_DOMAIN)));
            } else {
                rng.next();
            }
            if n % CITY_NULL_EVERY != 0 {
                let city = rng.pick(CITY_DOMAIN);
                p.insert("city".to_string(), unival!(city.to_string()));
            } else {
                rng.next();
            }
            p
        })
        .collect()
}

/// Builds one batch of `Company` property maps for indices `range`.
fn company_batch(range: std::ops::Range<usize>, rng: &mut Rng) -> Vec<HashMap<String, Value>> {
    range
        .map(|i| {
            let mut p: HashMap<String, Value> = HashMap::new();
            p.insert("name".to_string(), unival!(format!("c{i}")));
            if i as u64 % FOUNDED_NULL_EVERY != 0 {
                p.insert("founded".to_string(), unival!(rng.pick(FOUNDED_DOMAIN)));
            } else {
                rng.next();
            }
            p
        })
        .collect()
}

/// Inserts `total` vertices of `label` in [`VERTEX_BATCH`]-sized transactions,
/// returning every allocated [`Vid`] in insertion order.
async fn insert_vertices<F>(
    db: &Uni,
    label: &str,
    total: usize,
    mut build: F,
) -> anyhow::Result<Vec<Vid>>
where
    F: FnMut(std::ops::Range<usize>) -> Vec<HashMap<String, Value>>,
{
    let mut vids = Vec::with_capacity(total);
    let mut start = 0;
    while start < total {
        let end = (start + VERTEX_BATCH).min(total);
        let props = build(start..end);
        let tx = db.session().tx().await?;
        let batch = tx.bulk_insert_vertices(label, props).await?;
        tx.commit().await?;
        vids.extend(batch);
        start = end;
    }
    Ok(vids)
}

/// Builds a DQP fixture at `tier`.
///
/// The returned [`Uni`] owns its temporary directory — `Uni::temporary()` (which
/// `Uni::in_memory()` is a literal alias for, `api/mod.rs:785`) creates a real
/// on-disk `tempfile` dir and hands ownership to the builder, so there is no
/// `TempDir` for the caller to keep alive.
///
/// The build ends with `db.flush()`. Without it every row stays in L0, and the
/// L0-vs-L1 lever — along with any index the fixture is meant to engage — has
/// nothing to differentiate.
///
/// # Errors
///
/// Returns any error from database construction, schema application, or the
/// bulk inserts.
pub async fn build_dqp_seed(tier: Tier) -> anyhow::Result<Uni> {
    build_dqp_seed_with(tier, 0x5EED_D9F_u64.wrapping_mul(0x9E37_79B9)).await
}

/// [`build_dqp_seed`] with an explicit RNG seed, for replay harnesses.
///
/// # Errors
///
/// As [`build_dqp_seed`].
pub async fn build_dqp_seed_with(tier: Tier, seed: u64) -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;
    apply_schema(&db).await?;

    let mut rng = Rng::new(seed | 1);
    let person_vids =
        insert_vertices(&db, "Person", tier.persons(), |r| person_batch(r, &mut rng)).await?;

    let mut rng = Rng::new(seed.rotate_left(17) | 1);
    let company_vids = insert_vertices(&db, "Company", tier.companies(), |r| {
        company_batch(r, &mut rng)
    })
    .await?;

    // Persons eligible for edges — every EDGELESS_EVERY-th is skipped so
    // relationship patterns legitimately drop rows.
    let eligible: Vec<Vid> = person_vids
        .iter()
        .enumerate()
        .filter(|(i, _)| *i as u64 % EDGELESS_EVERY != 0)
        .map(|(_, v)| *v)
        .collect();

    let total_edges = tier.edges();
    let mut start = 0;
    while start < total_edges {
        let end = (start + EDGE_BATCH).min(total_edges);
        let edges: Vec<(Vid, Vid, HashMap<String, Value>)> = (start..end)
            .map(|i| {
                let src = eligible[i % eligible.len()];
                // Coprime stride: spreads companies without an RNG draw, so the
                // edge set is a pure function of the index.
                let dst = company_vids[(i * 7 + 13) % company_vids.len()];
                (src, dst, HashMap::new())
            })
            .collect();
        let tx = db.session().tx().await?;
        tx.bulk_insert_edges("WORKS_AT", edges).await?;
        tx.commit().await?;
        start = end;
    }

    db.flush().await?;
    Ok(db)
}

#[cfg(test)]
mod tests {
    use super::{Tier, build_dqp_seed};

    async fn count(db: &uni_db::Uni, q: &str) -> anyhow::Result<i64> {
        let r = db.session().query(q).await?;
        Ok(r.rows()[0].get::<i64>("c")?)
    }

    /// The fixture must have the cardinalities it claims, real NULLs, and — the
    /// part that actually matters — **non-trivial selectivity under the
    /// generator's hardcoded literals**. A fixture whose predicates all match
    /// everything or nothing would make DQP vacuous while looking healthy.
    #[tokio::test]
    async fn tiny_tier_has_expected_shape_and_selectivity() -> anyhow::Result<()> {
        let db = build_dqp_seed(Tier::Tiny).await?;
        let t = Tier::Tiny;

        assert_eq!(
            count(&db, "MATCH (p:Person) RETURN count(p) AS c").await?,
            t.persons() as i64,
        );
        assert_eq!(
            count(&db, "MATCH (c:Company) RETURN count(c) AS c").await?,
            t.companies() as i64,
        );
        assert_eq!(
            count(
                &db,
                "MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) AS c"
            )
            .await?,
            t.edges() as i64,
        );

        // Real NULLs — the IS NULL partition must be non-empty.
        let age_null = count(
            &db,
            "MATCH (p:Person) WHERE p.age IS NULL RETURN count(p) AS c",
        )
        .await?;
        assert!(age_null > 0, "age NULLs must exist, got {age_null}");

        // Selectivity under a literal the generator actually emits. `age = 30`
        // must match some rows but not all; either extreme makes DQP vacuous.
        let at_boundary = count(
            &db,
            "MATCH (p:Person) WHERE p.age = 30 RETURN count(p) AS c",
        )
        .await?;
        assert!(
            at_boundary > 0 && at_boundary < t.persons() as i64,
            "age = 30 must be selective, matched {at_boundary} of {}",
            t.persons()
        );

        // A literal the generator emits that must match nothing — always-false
        // predicates are a partition worth having.
        assert_eq!(
            count(
                &db,
                "MATCH (p:Person) WHERE p.city = 'ZZ' RETURN count(p) AS c"
            )
            .await?,
            0,
            "'ZZ' is deliberately outside the city domain"
        );
        Ok(())
    }
}
