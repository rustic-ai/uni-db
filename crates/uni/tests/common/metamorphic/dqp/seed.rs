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
use uni_db::{DataType, IndexType, ScalarType, Uni, Value, unival};

/// Boundary value for `age`, mirroring `metamorphic::seed::AGE_BOUNDARY`.
///
/// Several Persons sit exactly on it so off-by-one partitioning (`<` vs `<=`)
/// has something to catch.
pub const AGE_BOUNDARY: i64 = 30;

/// `age` values drawn by the fixture — a subset of `arb_literal`'s Int set that
/// reads as a plausible age, so `=`, `<` and `>` predicates all have matches.
pub const AGE_DOMAIN: &[i64] = &[18, 22, 25, 30, 40, 50, 65];
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

/// How a fixture declares its graph.
///
/// This is **not** cosmetic, and it is the dimension Phase 5's task list
/// missed. What a fixture declares decides which executor paths a generated
/// query can reach at all:
///
/// * Whether an edge type is declared decides `Traverse` vs
///   `TraverseMainByType` (`planner.rs:5405` routes to the latter only when
///   *every* requested type is absent from the schema). The #135 fix site,
///   `build_edge_adjacency_and_target_props`, has exactly one caller and it
///   lives under `TraverseMainByType` — so a fixture that declares its edge
///   type cannot reach that bug no matter how wide the generator gets.
/// * Whether a scalar index exists decides whether a predicate reaches
///   `LanceFilterGenerator` at all, which is where the deleted `"col"` range
///   fusion lived.
///
/// A widened *generator* over a fixture that routes around the defect produces
/// a wider, greener, still-toothless oracle. Widen the fixture too.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaMode {
    /// Labels and `WORKS_AT` declared, no indexes. What every Phase 2-4 lever
    /// measured against.
    Typed,
    /// [`Self::Typed`] plus a **Hash** scalar index on `Person.age`.
    ///
    /// The Hash kind specifically: an `=`/`IN` on a Hash-indexed column is what
    /// puts it into `PushdownStrategy::hash_index_columns`, which is condition
    /// (2) of three for reaching the range fusion. See
    /// `bugs/hash_index_range_quoting.rs:15-23`.
    TypedIndexed,
}

impl SchemaMode {
    /// Short name for test output and report tables.
    pub fn name(self) -> &'static str {
        match self {
            SchemaMode::Typed => "typed",
            SchemaMode::TypedIndexed => "typed-indexed",
        }
    }
}

/// A fixture identity: everything [`build_dqp_seed_for`] needs.
///
/// Carried instead of a bare [`Tier`] so a lever states both *how big* and
/// *what shape* its fixture is. The two are independent — an index changes
/// which code paths run, a tier changes how much data flows through them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fixture {
    pub tier: Tier,
    pub mode: SchemaMode,
}

impl Fixture {
    /// The Phase 2-4 default. Every existing lever uses this, so their measured
    /// budgets and activation rates carry over unchanged.
    pub const TINY: Self = Self {
        tier: Tier::Tiny,
        mode: SchemaMode::Typed,
    };

    /// [`Self::TINY`] with the Hash index on `Person.age`. For the pushdown
    /// lever.
    pub const TINY_INDEXED: Self = Self {
        tier: Tier::Tiny,
        mode: SchemaMode::TypedIndexed,
    };

    /// Label used in driver output, e.g. `tiny/typed-indexed`.
    pub fn name(self) -> String {
        format!("{}/{}", self.tier.name(), self.mode.name())
    }
}

impl From<Tier> for Fixture {
    /// A bare tier means the Phase 2-4 shape, so pre-Phase-5 call sites keep
    /// their exact semantics.
    fn from(tier: Tier) -> Self {
        Self {
            tier,
            mode: SchemaMode::Typed,
        }
    }
}

/// The property the [`SchemaMode::TypedIndexed`] Hash index covers.
///
/// `age` and not `score`/`city`: it is nullable (so three-valued logic stays
/// reachable), it is drawn from a small fixed domain [`AGE_DOMAIN`] that the
/// generator's literals also draw from, and it is the generator's most-used
/// numeric target — `numeric_targets`/`integer_targets` both surface it.
///
/// Aliased from the generator rather than repeated: `arb_case_pushdown` emits
/// its same-column conjunction over that column, and if the two ever named
/// different properties the predicate would miss the index entirely and the
/// lever would soak a path that cannot exhibit the defect — passing green.
pub const INDEXED_PROPERTY: &str = crate::querygen::PUSHDOWN_PROPERTY;

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

    /// Does this tier need a guaranteed base `WHERE` on every generated case?
    ///
    /// Above roughly 10k vertices an unfiltered case scans the whole fixture,
    /// and Phase 0A measured that two thirds of ordinary draws are unfiltered.
    /// See `querygen::arb_case_selective`.
    pub fn needs_selectivity_floor(self) -> bool {
        matches!(self, Tier::Smoke | Tier::Large)
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

/// Config every DQP fixture is built with.
///
/// Two overrides, one conventional and one load-bearing:
///
/// - `disable_fork_sweeper` — the established convention for deterministic fork
///   tests. Strictly unnecessary here, since a fork created without an explicit
///   TTL is never swept (`fork_default_ttl` defaults to `None`), but it removes
///   the 60 s background task rather than relying on that.
///
/// - `disable_fork_index_builder` — **required for a correct comparison.** The
///   background builder runs on a timer and can register a fork-local index
///   *partway through a run*. A Tier-2 lever's whole premise is that state does
///   not change during the case loop; if an index appears at case 200, the first
///   200 cases and the remaining 300 exercise different machinery, and nothing
///   in the output would say so. It also registers its own fork holder, which
///   makes teardown timing nondeterministic.
///
/// - `auto_flush_interval: None` — **required for the Tier-1 flush lever, and
///   measured rather than anticipated.** The default is
///   `Some(Duration::from_secs(5))` with `auto_flush_min_mutations: 1`
///   (`uni-common/src/config.rs:431,443`), so a background timer drains L0 five
///   seconds after any write. `drive_stateful`'s pass 1 over 500 cases takes ~34
///   s, so the timer fired a quarter of the way through and every case after it
///   compared a flushed state against a flushed state. The first run measured
///   **25.8% activation** for a witness that scored 60/60 when checked directly —
///   the activation floor catching a background task, which is the same hazard
///   `disable_fork_index_builder` exists for.
///
///   This is not merely a flush-lever concern: any lever whose side A depends on
///   L0 residency is exposed, which is why it is pinned here for every DQP
///   fixture rather than in one lever's `prepare`.
fn dqp_config() -> uni_db::UniConfig {
    uni_db::UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        auto_flush_interval: None,
        ..Default::default()
    }
}

/// Applies the `querygen`-mandated schema, in the shape `mode` asks for.
async fn apply_schema(db: &Uni, mode: SchemaMode) -> anyhow::Result<()> {
    let person = db
        .schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int)
        .property_nullable("score", DataType::Float)
        .property_nullable("city", DataType::String);

    // Declared before any row is inserted, matching how
    // `bugs/hash_index_range_quoting.rs` reaches the pushdown path. A Hash
    // scalar index needs no physical Lance artifact — `ScalarIndexType::Hash`
    // has no Lance counterpart and is mapped onto BTree — so what makes
    // pushdown engage is the *declaration*, not a build. That is why this is
    // safe to declare up front and why the self-tests below check behaviour
    // rather than trusting `apply()` returning `Ok`.
    let person = match mode {
        SchemaMode::Typed => person,
        SchemaMode::TypedIndexed => {
            person.index(INDEXED_PROPERTY, IndexType::Scalar(ScalarType::Hash))
        }
    };

    person
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
    build_dqp_seed_for(Fixture::from(tier)).await
}

/// [`build_dqp_seed`] with an explicit RNG seed, for replay harnesses.
///
/// # Errors
///
/// As [`build_dqp_seed`].
pub async fn build_dqp_seed_with(tier: Tier, seed: u64) -> anyhow::Result<Uni> {
    build_dqp_seed_for_with(Fixture::from(tier), seed).await
}

/// Builds the fixture `f` describes.
///
/// # Errors
///
/// As [`build_dqp_seed`].
pub async fn build_dqp_seed_for(f: Fixture) -> anyhow::Result<Uni> {
    build_dqp_seed_for_with(f, 0x5EED_D9F_u64.wrapping_mul(0x9E37_79B9)).await
}

/// [`build_dqp_seed_for`] with an explicit RNG seed, for replay harnesses.
///
/// The seed drives only the *data*, never the schema, so two fixtures that
/// differ solely in [`SchemaMode`] hold byte-identical rows. That is what lets
/// a pushdown run be compared against a non-indexed one without the data
/// varying underneath.
///
/// # Errors
///
/// As [`build_dqp_seed`].
pub async fn build_dqp_seed_for_with(f: Fixture, seed: u64) -> anyhow::Result<Uni> {
    let tier = f.tier;
    let db = Uni::temporary().config(dqp_config()).build().await?;
    apply_schema(&db, f.mode).await?;

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
    use super::{Fixture, INDEXED_PROPERTY, Tier, build_dqp_seed, build_dqp_seed_for, dqp_config};

    /// The background flush timer must stay off for every DQP fixture.
    ///
    /// This one is worth an assertion rather than a comment because it is a
    /// `..Default::default()` away from coming back, and its return would not
    /// break anything visibly: the Tier-2 levers would carry on passing, and the
    /// flush lever would report a degraded activation rate that reads like
    /// witness drift rather than a background task. It cost a full debugging
    /// cycle to find the first time — 25.8% activation for a witness that
    /// measured 60/60 correct in isolation.
    ///
    /// Asserted on the config rather than behaviourally, i.e. rather than
    /// inserting a delta and waiting out the 5-second timer: the wait would put
    /// six idle seconds in the PR lane to observe a fact the config states
    /// outright.
    #[test]
    fn dqp_fixtures_disable_the_background_flush_timer() {
        assert!(
            dqp_config().auto_flush_interval.is_none(),
            "the time-based L0 flush is enabled for DQP fixtures. Any lever whose \
             side A depends on L0 residency will silently compare a flushed state \
             against a flushed state once the timer fires mid-run."
        );
    }

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

    /// The two fixtures must hold identical data, so a pushdown run and a
    /// non-indexed run differ in exactly one variable.
    #[tokio::test]
    async fn schema_mode_does_not_change_the_data() -> anyhow::Result<()> {
        let plain = build_dqp_seed_for(Fixture::TINY).await?;
        let indexed = build_dqp_seed_for(Fixture::TINY_INDEXED).await?;

        for q in [
            "MATCH (p:Person) RETURN count(p) AS c",
            "MATCH (p:Person) WHERE p.age = 30 RETURN count(p) AS c",
            "MATCH (p:Person) WHERE p.age IS NULL RETURN count(p) AS c",
            "MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) AS c",
        ] {
            assert_eq!(
                count(&plain, q).await?,
                count(&indexed, q).await?,
                "SchemaMode changed the data, not just the access path: {q}"
            );
        }
        Ok(())
    }

    /// The declared index must be **registered and healthy**, not merely
    /// accepted by `apply()`.
    ///
    /// `apply()` returning `Ok` proves nothing here: a physical build failure is
    /// swallowed into a `warn!` and the definition is registered anyway, and
    /// `IndexStatus::Online` is the `#[default]` — so an index gets `Online` for
    /// *existing*, not for being usable. If this fixture's index were silently
    /// dead, `TypedIndexed` would be indistinguishable from `Typed` and every
    /// pushdown case would be vacuous while the suite stayed green.
    #[tokio::test]
    async fn indexed_fixture_registers_its_index() -> anyhow::Result<()> {
        use uni_db::core::schema::{IndexDefinition, ScalarIndexType};

        /// A **Hash** scalar index covering `age`. The kind matters as much as
        /// the column: only `=`/`IN` against a Hash-indexed column populates
        /// `PushdownStrategy::hash_index_columns`, so a BTree here would leave
        /// the lever unable to reach the fusion.
        fn is_the_hash_index(i: &IndexDefinition) -> bool {
            matches!(
                i,
                IndexDefinition::Scalar(c)
                    if c.index_type == ScalarIndexType::Hash
                        && c.properties.iter().any(|p| p == INDEXED_PROPERTY)
            )
        }

        let db = build_dqp_seed_for(Fixture::TINY_INDEXED).await?;
        let found = db
            .indexes()
            .list(Some("Person"))
            .into_iter()
            .find(is_the_hash_index)
            .unwrap_or_else(|| {
                panic!("no Hash index on Person.{INDEXED_PROPERTY} — TypedIndexed built nothing")
            });
        assert_eq!(
            format!("{:?}", found.metadata().status),
            "Online",
            "the fixture's Hash index is not Online, so pushdown will not engage"
        );

        // And the plain fixture must genuinely lack it, or the two modes are
        // the same fixture wearing different names.
        let plain = build_dqp_seed_for(Fixture::TINY).await?;
        assert!(
            !plain
                .indexes()
                .list(Some("Person"))
                .iter()
                .any(is_the_hash_index),
            "SchemaMode::Typed carries the index too — the modes do not differ"
        );
        Ok(())
    }

    /// **The routing guard.**
    ///
    /// A fixture can hold the right data, run the right transition, and still
    /// route every generated query around the code path under test. That is not
    /// hypothetical: `build_edge_adjacency_and_target_props` (the #135 fix site)
    /// has exactly one caller, under `GraphTraverseMainStream`, which is planned
    /// only when *every* requested relationship type is absent from the schema
    /// (`planner.rs:5405`). These fixtures declare `WORKS_AT`, so they plan
    /// through typed `Traverse` and **cannot reach #135 at all** — no amount of
    /// generator widening changes that.
    ///
    /// Asserting the operator makes that property checkable instead of assumed,
    /// and turns a future "we made the fixture schemaless" into a passing test
    /// rather than a silent hope.
    ///
    /// Substring containment inside one process only: plan text is
    /// `format!("{:#?}", plan)` over structures with `HashSet`/`HashMap` fields
    /// and is not ordering-stable across processes.
    #[tokio::test]
    async fn typed_fixture_plans_a_typed_traverse() -> anyhow::Result<()> {
        let db = build_dqp_seed_for(Fixture::TINY).await?;
        let r = db
            .session()
            .query("EXPLAIN MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN b.founded")
            .await?;
        let plan: String = r.rows()[0].get("plan")?;
        assert!(
            !plan.contains("TraverseMainByType"),
            "the fixture declares WORKS_AT, so it must plan a typed Traverse — if \
             this now says TraverseMainByType the fixture went schemaless, and the \
             levers are exercising a different operator than they were measured \
             against:\n{plan}"
        );
        Ok(())
    }

    /// **The condition the pushdown lever depends on.**
    ///
    /// Reaching the deleted `"col"` range fusion needs all three of: a Hash
    /// index on the column, an `=`/`IN` on it, and a two-sided *inclusive*
    /// range on the same column (`bugs/hash_index_range_quoting.rs:15-23`).
    /// This asserts the fixture supplies the first and that the resulting query
    /// is both **correct** and **non-empty**.
    ///
    /// Non-emptiness is the load-bearing half. The fusion's symptom is an empty
    /// result, so a case that legitimately matches nothing passes identically
    /// with and without the bug — exactly why
    /// `range_still_constrains_when_it_excludes_the_equality` cannot detect it.
    /// A fixture whose pushdown queries are all empty would give the lever
    /// nothing to see.
    #[tokio::test]
    async fn indexed_fixture_reaches_the_pushdown_path() -> anyhow::Result<()> {
        let db = build_dqp_seed_for(Fixture::TINY_INDEXED).await?;

        // `age = 30` with a range that contains 30 — the eq-plus-two-sided-range
        // shape, and the answer must equal the plain equality's answer.
        let eq_only = count(
            &db,
            "MATCH (p:Person) WHERE p.age = 30 RETURN count(p) AS c",
        )
        .await?;
        let fused = count(
            &db,
            "MATCH (p:Person) WHERE p.age = 30 AND p.age >= 25 AND p.age <= 40 \
             RETURN count(p) AS c",
        )
        .await?;
        assert!(eq_only > 0, "fixture has no age=30 rows to push down");
        assert_eq!(
            fused, eq_only,
            "eq + containing range must equal eq alone; got {fused} vs {eq_only}"
        );

        // The `IN` route into `hash_index_columns` takes the same path.
        let in_form = count(
            &db,
            "MATCH (p:Person) WHERE p.age IN [25, 30, 40] AND p.age >= 25 AND p.age <= 40 \
             RETURN count(p) AS c",
        )
        .await?;
        assert!(
            in_form > 0,
            "IN + range matched nothing, so the lever would compare empty bags"
        );
        Ok(())
    }
}
