//! Phase 4A — do the Tier-1 transitions have activation witnesses at all?
//!
//! Phase 4 proposes four stateful levers: flush (L0→L1), index absent→present,
//! plan cache cold→warm, and pre→post compaction. Each needs an **activation
//! witness** — an observable proving side A and side B genuinely ran different
//! machinery — or the lever is vacuously green, which is the single failure mode
//! this whole oracle exists to prevent (`lever.rs`, module docs).
//!
//! The Tier-2 levers had their witnesses handed to them: `branch_scans` and
//! `snapshot_reads` were added in Phase 1 precisely because a fork read and a
//! pinned read are observable. **Nothing in `QueryMetrics` observes an index
//! lookup or a fragment layout.** Whether the remaining two transitions can be
//! witnessed at all is therefore an open question, and building a lever on the
//! assumption that they can is how a harness ends up reporting a clean green run
//! forever.
//!
//! These began as **report-only probes**, in the shape of Phase 0A's feasibility
//! measurements: run the transition, print every counter on both sides, assert
//! nothing that had not already been measured. The answers came back split —
//! flush and plan cache are witnessable, index and compaction are not — so the
//! file now holds two kinds of test.
//!
//! The flush and plan-cache probes stay diagnostic: they print the per-clause
//! tally a witness is built from, which is what identified the background
//! auto-flush timer when `flush_smoke` reported 25.8% activation for a witness
//! that measured 60/60 correct in isolation.
//!
//! The index and compaction probes **were** tripwires: they asserted the
//! unobservability that justified deferring those two levers, so a deferral
//! recorded in a document could not quietly become false. Both deferrals have
//! since expired and both probes now assert the measurement instead — index
//! consultation became observable with #175, compaction with the `lance_iops`
//! counter. What is left of the original role is the shape: each still pairs a
//! must-move case with a must-not-move one, because a probe that only ever
//! asserts one direction passes for free.
//!
//! Run with output:
//!
//! ```text
//! cargo nextest run -p uni-db --test integration --no-capture \
//!   -E 'test(/dqp::transition_probe/)'
//! ```

use std::collections::HashMap;

use uni_db::{Session, Uni, Value, unival};
use uni_store::compaction::CompactionStats;

use super::lever::{Witness, witness_of};
use super::seed::{Fixture, Tier, build_dqp_seed, build_dqp_seed_tuned};

/// Queries every probe runs, chosen to span the shapes the generator emits: an
/// unfiltered node scan, a filtered node scan on an indexable Int property, a
/// filtered scan on a String property, and an edge traversal.
///
/// Hand-written rather than generated: a probe reporting a counter needs the
/// query text in the output to be readable, and four fixed shapes make the
/// before/after columns line up.
const PROBE_QUERIES: &[(&str, &str)] = &[
    ("node-all", "MATCH (a:Person) RETURN a.name"),
    (
        "node-filtered-int",
        "MATCH (a:Person) WHERE a.age > 30 RETURN a.name",
    ),
    (
        "node-filtered-str",
        "MATCH (a:Person) WHERE a.city = 'NYC' RETURN a.name",
    ),
    (
        "edge",
        "MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN a.name, b.name",
    ),
];

/// Runs `cypher` on `session` and returns the witness plus the row count.
async fn probe(session: &Session, cypher: &str) -> anyhow::Result<(Witness, usize)> {
    let r = session.query(cypher).await?;
    let rows = r.rows().len();
    Ok((witness_of(&r), rows))
}

/// Prints one side's counters in a fixed-width row.
fn report(label: &str, name: &str, w: &Witness, rows: usize) {
    println!(
        "  {label:<8} {name:<18} rows={rows:<7} l0={:<7} storage={:<7} scanned={:<8} \
         branch={} snapshot={} index_scans={}",
        w.l0_reads,
        w.storage_reads,
        w.rows_scanned,
        w.branch_scans,
        w.snapshot_reads,
        w.index_scans
    );
}

/// Inserts `n` extra `Person` rows and leaves them in L0 (no flush).
///
/// Values come from the same domains [`super::seed`] uses, so the delta does not
/// change predicate selectivity — the property the tiers are built around.
async fn insert_l0_delta(db: &Uni, n: usize) -> anyhow::Result<()> {
    let rows: Vec<HashMap<String, Value>> = (0..n)
        .map(|i| {
            let mut p = HashMap::new();
            p.insert("name".to_string(), unival!(format!("d{i}")));
            p.insert("age".to_string(), unival!([18i64, 25, 30, 40, 65][i % 5]));
            p.insert(
                "city".to_string(),
                unival!(["NYC", "SF", "LA"][i % 3].to_string()),
            );
            p
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.bulk_insert_vertices("Person", rows).await?;
    tx.commit().await?;
    Ok(())
}

/// Compacts `label` until a pass reports no work, returning that final pass.
///
/// A single `compact` is not guaranteed to merge everything: the planner groups
/// fragments into bins and can leave some behind, so "already compacted" is a
/// fixed point rather than a one-call property. Bounded so a planner that never
/// converges fails loudly instead of hanging.
async fn compact_until_converged(db: &Uni, label: &str) -> anyhow::Result<CompactionStats> {
    Ok(compact_to_fixpoint(db, label).await?.1)
}

/// Compacts to a fixpoint, returning `(total fragments removed, the final pass)`.
///
/// **A single `compact` is not enough, and measurably so.** After an insert and
/// a flush, the first compaction reports no work and a second one immediately
/// after merges two fragments into one — with nothing happening in between, so
/// the lag is not time-based. It reproduces on every round:
///
/// ```text
/// round 0 AFTER-insert compact#1: removed=0 added=0
/// round 0 AFTER-insert compact#2: removed=2 added=1
/// ```
///
/// So compaction runs one flush behind. That is a pre-existing behaviour this
/// test cannot fix and did not cause — it was simply invisible while
/// `CompactionStats` returned literals. Tests here assert the fixpoint, which is
/// deterministic, rather than single-pass completeness, which is not.
async fn compact_to_fixpoint(db: &Uni, label: &str) -> anyhow::Result<(usize, CompactionStats)> {
    let mut total_removed = 0;
    let mut quiet = 0;
    let mut last = CompactionStats::default();
    for _ in 0..10 {
        last = db.compaction().compact(label).await?;
        db.compaction().wait().await?;
        if last.fragments_removed == 0 {
            // Two consecutive quiet passes, not one. Because of the one-flush
            // lag above, the pass immediately after a flush always reports zero
            // — stopping there would return before any merge had happened and
            // report a converged, untouched table.
            quiet += 1;
            if quiet == 2 {
                return Ok((total_removed, last));
            }
        } else {
            quiet = 0;
            total_removed += last.fragments_removed;
        }
    }
    anyhow::bail!("compaction of '{label}' did not converge in 10 passes; last={last:?}")
}

/// **Probe 1 — flush.** Does an unflushed delta show up as `l0_reads`, and does
/// flushing drive it to zero?
///
/// This is the one transition whose witness is expected to already exist:
/// `l0_reads` and `storage_reads` were populated in Phase 1 and are asserted by
/// `counters.rs`. What is *not* known is whether a query over a mixed L0+L1
/// table reports both, which is what a `l0>0 → l0==0` activation predicate needs.
#[tokio::test(flavor = "multi_thread")]
async fn probe_flush_transition() -> anyhow::Result<()> {
    let db = build_dqp_seed(Tier::Tiny).await?;
    insert_l0_delta(&db, 200).await?;

    println!("\n[probe:flush] side A = mixed L0+L1, side B = all L1");
    let session = db.session();
    let mut before = Vec::new();
    for (name, q) in PROBE_QUERIES {
        let (w, rows) = probe(&session, q).await?;
        report("before", name, &w, rows);
        before.push((*name, w, rows));
    }

    db.flush().await?;

    // A fresh session: the same one would answer from its warm plan cache, which
    // is a *different* transition and would confound this one.
    let session = db.session();
    for ((name, w_before, rows_before), (_, q)) in before.iter().zip(PROBE_QUERIES) {
        let (w_after, rows_after) = probe(&session, q).await?;
        report("after", name, &w_after, rows_after);
        assert_eq!(
            *rows_before, rows_after,
            "[{name}] flush changed the row count — that is a bug, not a probe result"
        );
        println!(
            "  {:<8} {name:<18} l0 {} -> {}   storage {} -> {}   ACTIVATES={}",
            "delta",
            w_before.l0_reads,
            w_after.l0_reads,
            w_before.storage_reads,
            w_after.storage_reads,
            w_before.l0_reads > 0 && w_after.l0_reads == 0
        );
    }
    Ok(())
}

/// **Probe 2 — plan cache.** Is a fresh session's cache genuinely cold, and does
/// a second execution of the same text register a hit?
///
/// `QueryResult::plan_cache_hit` is a write-path-only field, permanently `false`
/// on the read path — measured in Phase 0A and pinned by a test in
/// `feasibility.rs`. So the witness must come from `SessionMetrics`, and the
/// question is whether the delta is observable per query.
///
/// If it is, this lever is **not stateful at all**: the plan cache is per
/// `Session` (`session.rs:202,257`), so cold and warm are two sessions rather
/// than two points in time, and it belongs in `drive_prepared` with the other
/// Tier-2 levers.
#[tokio::test(flavor = "multi_thread")]
async fn probe_plan_cache_transition() -> anyhow::Result<()> {
    let db = build_dqp_seed(Tier::Tiny).await?;

    println!("\n[probe:plan-cache] cold session vs the same session re-running");
    for (name, q) in PROBE_QUERIES {
        let session = db.session();
        let before = session.metrics();
        session.query(q).await?;
        let mid = session.metrics();
        session.query(q).await?;
        let after = session.metrics();

        println!(
            "  {name:<18} 1st: hits+{} misses+{}   2nd: hits+{} misses+{}   size={}",
            mid.plan_cache_hits - before.plan_cache_hits,
            mid.plan_cache_misses - before.plan_cache_misses,
            after.plan_cache_hits - mid.plan_cache_hits,
            after.plan_cache_misses - mid.plan_cache_misses,
            after.plan_cache_size,
        );
    }

    // And a genuinely fresh session must start cold, or "cold vs warm" is not a
    // distinction the API can express.
    let fresh = db.session();
    println!(
        "  fresh session: hits={} misses={} size={}",
        fresh.metrics().plan_cache_hits,
        fresh.metrics().plan_cache_misses,
        fresh.metrics().plan_cache_size,
    );
    Ok(())
}

/// The index transition **is** observable, for the shape that reaches pushdown.
///
/// This began as a tripwire asserting the opposite, and it passed — for the
/// wrong reason. It created a **BTree** index and probed it with `age > 30`,
/// but `build_indexed_property_pushdown` collects only Hash-indexed `=`/`IN`,
/// so that shape hands Lance no predicate an index could serve. The probe used
/// a query that structurally cannot consult an index, observed nothing, and
/// concluded index usage was unobservable. A non-discriminating probe passes
/// for free.
///
/// Rebuilt around a **Hash** index on `city`, which `node-filtered-str` already
/// queries with `=`. The expectation is now per-shape, because a single
/// whole-`Witness` comparison cannot express "one of these four must change and
/// the other three must not" — and that asymmetry is the actual claim.
///
/// `index_scans` comes from Lance's execution-stats callback (#175), so it
/// reports what the plan did, not what was declared.
#[tokio::test(flavor = "multi_thread")]
async fn index_transition_is_observable_for_hash_equality_only() -> anyhow::Result<()> {
    use uni_db::api::schema::{IndexType, ScalarType};

    // Only the `=`-on-indexed-column shape may consult an index.
    fn expects_index(name: &str) -> bool {
        name == "node-filtered-str"
    }

    let db = build_dqp_seed(Tier::Tiny).await?;

    println!("\n[probe:index] side A = no index on Person.city, side B = Hash on it");
    let session = db.session();
    let mut before = Vec::new();
    for (name, q) in PROBE_QUERIES {
        let (w, rows) = probe(&session, q).await?;
        report("before", name, &w, rows);
        assert_eq!(
            w.index_scans, 0,
            "[{name}] consulted an index before one was created"
        );
        before.push((*name, w, rows));
    }

    db.schema()
        .label("Person")
        .index("city", IndexType::Scalar(ScalarType::Hash))
        .apply()
        .await?;
    println!("  indexes now: {:?}", db.indexes().list(Some("Person")));

    let session = db.session();
    for ((name, _w_before, rows_before), (_, q)) in before.iter().zip(PROBE_QUERIES) {
        let (w_after, rows_after) = probe(&session, q).await?;
        report("after", name, &w_after, rows_after);

        assert_eq!(
            *rows_before, rows_after,
            "[{name}] creating an index changed the row count — that is a bug, not a \
             probe result, and it is exactly what the index lever would be built to catch"
        );

        if expects_index(name) {
            assert!(
                w_after.index_scans > 0,
                "[{name}] an `=` on a freshly Hash-indexed column did not consult the \
                 index. Either the index was declared but never physically built, or \
                 the pushdown route regressed. witness: {w_after:?}"
            );
        } else {
            assert_eq!(
                w_after.index_scans, 0,
                "[{name}] consulted an index for a shape that cannot reach pushdown. \
                 If a BTree/range route was added, this test's expectation table is \
                 stale rather than wrong. witness: {w_after:?}"
            );
        }
    }
    Ok(())
}

/// **Compaction is observable at run level, and only there.**
///
/// This was a tripwire asserting the placeholder — `files_compacted: 1,
/// bytes_before: 0, bytes_after: 0` — and it has now fired. #172 made
/// `CompactionStats` report measured work, so the run-level half of the
/// deferral has expired and this test asserts the measurement instead.
///
/// The *lever* half expired separately, and later: `Witness` is per-query
/// (`lever.rs`) while `CompactionStats` is per-run, so run-level honesty could
/// not feed `activated(&Witness, &Witness)`. What could was `lance_iops`, and
/// the second half below — once a tripwire asserting that nothing moved — now
/// asserts that it is the *only* thing that moves.
///
/// Background compaction is disabled for the fixture. The loop fires at
/// `l1_runs >= 8` on a 10 s tick, and a merge behind this test's back would
/// leave `fragments_removed` at zero on what should be a real compaction — and
/// `compact_label` would additionally contend for the `CompactionGuard`.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_is_observable_at_run_level_only() -> anyhow::Result<()> {
    let db = build_dqp_seed_tuned(Fixture::TINY, 0, |c| c.compaction.enabled = false).await?;
    // Several small flushed batches, so there is something to compact.
    for _ in 0..4 {
        insert_l0_delta(&db, 100).await?;
        db.flush().await?;
    }

    println!("\n[probe:compaction] side A = many fragments, side B = compacted");
    let session = db.session();
    let mut before = Vec::new();
    for (name, q) in PROBE_QUERIES {
        let (w, rows) = probe(&session, q).await?;
        report("before", name, &w, rows);
        before.push((*name, w, rows));
    }

    let real = db.compaction().compact("Person").await?;
    db.compaction().wait().await?;
    println!("  real:  {real:?}");

    // `compact_label` visits exactly the Person vertex table.
    assert_eq!(real.tables_optimized, 1);
    // The merge happened. `>= 2`, not an exact count: fragments-per-flush is an
    // implementation detail, not a contract.
    assert!(
        real.fragments_removed >= 2,
        "no fragments were merged — either the fixture did not accumulate them \
         or a background compaction beat this call. stats: {real:?}"
    );
    assert!(
        real.fragments_added >= 1 && real.fragments_added < real.fragments_removed,
        "compaction did not reduce the fragment count: {real:?}"
    );
    assert_eq!(
        real.files_added, real.fragments_added,
        "files_added is documented as equal to fragments added: {real:?}"
    );
    assert!(
        real.files_removed >= real.fragments_removed,
        "files_removed also counts deletion files, so it can exceed but never \
         trail fragments_removed: {real:?}"
    );
    assert!(real.duration > std::time::Duration::ZERO);
    // A documented zero, not a placeholder: version pruning only reclaims past
    // the retention window, and this fixture is seconds old.
    assert_eq!(real.bytes_reclaimed, 0);
    // compact_label runs no semantic pass, so crdt_merges is not measured here
    // — which is exactly what semantic_passes == 0 says.
    assert_eq!((real.semantic_passes, real.crdt_merges), (0, 0));

    // The discrimination: compact until it converges, then once more. The
    // converged call must report no work while still reporting the table as
    // visited.
    //
    // One pass is not enough, and that is a property of the planner rather than
    // a flake: it groups fragments into bins and can leave some unmerged, so
    // five fragments went 4 -> 1 above and left two behind. Convergence is what
    // is deterministic here, not single-pass completeness.
    let noop = compact_until_converged(&db, "Person").await?;
    println!("  no-op: {noop:?}");
    assert_eq!(
        (
            noop.fragments_removed,
            noop.fragments_added,
            noop.files_removed,
            noop.files_added
        ),
        (0, 0, 0, 0),
        "a compaction over an already-merged table reported work. Either the \
         fixture left deletion files, which make a lone fragment compact \
         itself, or these numbers are not coming from the storage layer: {noop:?}"
    );
    // Deliberately still 1: the table was visited. This is the denominator, not
    // a discriminator — do not "fix" it to 0.
    assert_eq!(noop.tables_optimized, 1);
    assert_ne!(
        real.fragments_removed, noop.fragments_removed,
        "a real compaction and a no-op report identically"
    );

    let session = db.session();
    for ((name, w_before, rows_before), (_, q)) in before.iter().zip(PROBE_QUERIES) {
        let (w_after, rows_after) = probe(&session, q).await?;
        report("after", name, &w_after, rows_after);
        assert_eq!(
            *rows_before, rows_after,
            "[{name}] compaction changed the row count — that is a bug, not a probe result"
        );
        // Exactly one counter may move, and it is the lever's witness.
        //
        // This half was a tripwire asserting that *nothing* moved, which is what
        // kept the compaction lever deferred. It has since fired — `lance_iops`
        // drops because a compacted table takes fewer physical reads — so it now
        // asserts the shape of that movement instead.
        assert!(
            w_after.lance_iops < w_before.lance_iops,
            "[{name}] lance_iops did not fall across a compaction, so the \
             compaction lever's witness has stopped working.\n\
             before: {w_before:?}\nafter:  {w_after:?}"
        );
        // Everything else must be identical: compaction changes how rows are
        // found, never which. A moving row count here would be a bug, not a
        // witness.
        assert_eq!(
            Witness {
                lance_iops: 0,
                ..*w_before
            },
            Witness {
                lance_iops: 0,
                ..w_after
            },
            "[{name}] a counter other than lance_iops moved across compaction.\n\
             before: {w_before:?}\nafter:  {w_after:?}"
        );
    }
    Ok(())
}

/// **Probe 5 — does a deletion move any per-query counter?**
///
/// The deletion lever (#183) needs an `activated` predicate, and the trait has
/// no default for one on purpose. Rather than guess which `Witness` field moves
/// when rows go away, measure it — the same order the compaction lever
/// followed, where `iops` was probed before anything was built on it.
///
/// Unlike the flush, index and compaction transitions, a deletion changes the
/// *data* rather than the physical layout, so the row counters are the obvious
/// candidates and the I/O counters are the interesting ones. If nothing moves,
/// the lever ships on its narrowing rate and says so, rather than returning a
/// constant from `activated`.
#[tokio::test(flavor = "multi_thread")]
async fn probe_delete_transition() -> anyhow::Result<()> {
    let db = build_dqp_seed_tuned(Fixture::TINY, 0, |c| c.compaction.enabled = false).await?;

    println!("\n[probe:delete] side A = full fixture, side B = a slice deleted");
    let session = db.session();
    let mut before = Vec::new();
    for (name, q) in PROBE_QUERIES {
        let (w, rows) = probe(&session, q).await?;
        report("before", name, &w, rows);
        before.push((*name, w, rows));
    }

    // Deterministic slice, not a random one: `replay_stateful` reproduces a
    // failure from a seed, so a transition that varies run to run would print
    // coordinates that reproduce nothing.
    let tx = db.session().tx().await?;
    let deleted = tx
        .execute("MATCH (a:Person) WHERE a.age = 18 DETACH DELETE a")
        .await?;
    tx.commit().await?;
    println!("  deleted: {deleted:?}");

    let session = db.session();
    let mut moved = Vec::new();
    for ((name, w_before, rows_before), (_, q)) in before.iter().zip(PROBE_QUERIES) {
        let (w_after, rows_after) = probe(&session, q).await?;
        report("after", name, &w_after, rows_after);
        if w_before != &w_after {
            moved.push((*name, *w_before, w_after));
        }
        assert!(
            rows_after <= *rows_before,
            "[{name}] a deletion returned MORE rows than before: {rows_before} -> {rows_after}"
        );
    }

    println!(
        "\n  queries whose witness moved: {}/{}",
        moved.len(),
        PROBE_QUERIES.len()
    );
    for (name, b, a) in &moved {
        println!(
            "    {name}: rows_scanned {} -> {}, l0 {} -> {}, storage {} -> {}, iops {} -> {}",
            b.rows_scanned,
            a.rows_scanned,
            b.l0_reads,
            a.l0_reads,
            b.storage_reads,
            a.storage_reads,
            b.lance_iops,
            a.lance_iops
        );
    }
    Ok(())
}

/// **Probe 1b — which clause of the flush witness fails on generated cases?**
///
/// The four hand-written queries in [`PROBE_QUERIES`] activated the flush lever's
/// three-clause witness 4 times out of 4. Generated cases activated it 129 times
/// out of 500. Hand-written probes are a biased sample of query shapes, so the
/// witness has to be checked against the population it will actually see.
///
/// This tallies each clause separately over real generated cases, so the failing
/// clause is identified rather than guessed at.
#[tokio::test(flavor = "multi_thread")]
async fn probe_flush_witness_over_generated_cases() -> anyhow::Result<()> {
    use crate::metamorphic::dqp::driver::{CaseKind, Db, strategy_for};
    use crate::metamorphic::dqp::flush_lever::FlushLever;
    use crate::metamorphic::dqp::stateful::StatefulLever;
    use crate::querygen::render::render;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;

    const N: usize = 60;

    let mut runner = TestRunner::deterministic();
    let strategy = strategy_for(CaseKind::Plain, Tier::Tiny);
    let cases: Vec<_> = (0..N)
        .map(|_| strategy.new_tree(&mut runner).unwrap().current())
        .collect();

    let db: Db = std::sync::Arc::new(build_dqp_seed(Tier::Tiny).await?);
    let mut lever = FlushLever::prepare(&db).await?;

    let mut before = Vec::new();
    for c in &cases {
        before.push(lever.observe_now(&c.base_query()).await?);
    }
    lever.transition().await?;

    let (mut c1, mut c2, mut c3, mut all) = (0, 0, 0, 0);
    let mut shown = 0;
    println!("\n[probe:flush-generated] per-clause tally over {N} generated cases");
    for (a, c) in before.iter().zip(&cases) {
        let b = lever.observe_now(&c.base_query()).await?;
        let (aw, bw) = (&a.witness, &b.witness);
        let k1 = aw.l0_reads > 0;
        let k2 = bw.l0_reads == 0;
        let k3 = bw.storage_reads > aw.storage_reads;
        c1 += usize::from(k1);
        c2 += usize::from(k2);
        c3 += usize::from(k3);
        all += usize::from(k1 && k2 && k3);
        if !(k1 && k2 && k3) && shown < 8 {
            shown += 1;
            println!(
                "  MISS a.l0>0={k1:<5} b.l0==0={k2:<5} b.stor>a.stor={k3:<5}  \
                 a(l0={},stor={},scan={}) b(l0={},stor={},scan={})\n    {}",
                aw.l0_reads,
                aw.storage_reads,
                aw.rows_scanned,
                bw.l0_reads,
                bw.storage_reads,
                bw.rows_scanned,
                render(&c.base_query()),
            );
        }
    }
    println!(
        "  a.l0>0: {c1}/{N}   b.l0==0: {c2}/{N}   b.storage>a.storage: {c3}/{N}   all three: {all}/{N}"
    );
    Ok(())
}

/// The **logical** plan never shows index selection, permanently and by design.
///
/// This was written as a tripwire — "if the plan text changes, the index lever
/// has a witness" — and it can never fire, which makes it the more dangerous of
/// the two. Index pushdown is decided in the *physical* planner
/// (`build_indexed_property_pushdown`, attached to `GraphScanExec` as an extra
/// Lance filter), and `ExplainOutput::plan_text` is `format!("{:#?}")` over the
/// `LogicalPlan`. The two cannot meet. A "good news" message that is
/// unreachable is worse than no message: it advertises a route to the witness
/// that does not exist, and the next person to look for one will try plan text
/// again.
///
/// Kept, inverted, and renamed, because the property is worth pinning. There
/// are now three distinct reports and they should not be confused:
///
/// | layer | what it says |
/// |---|---|
/// | `LogicalPlan` text | nothing about indexes — asserted here |
/// | `ExplainOutput::index_usage` | the *planner's prediction*; `used: true` is hardcoded |
/// | `QueryMetrics::index_scans` | what the *executed* plan did (#175) |
#[tokio::test(flavor = "multi_thread")]
async fn logical_plan_does_not_show_index_selection() -> anyhow::Result<()> {
    use uni_db::api::schema::{IndexType, ScalarType};

    const Q: &str = "MATCH (a:Person) WHERE a.age > 30 RETURN a.name";

    let db = build_dqp_seed(Tier::Tiny).await?;

    // A fresh session each time: a warm plan cache would return the *stored*
    // plan and prove nothing about what the planner would now choose.
    let explain = async |db: &Uni| -> anyhow::Result<String> {
        let r = db.session().query(&format!("EXPLAIN {Q}")).await?;
        Ok(r.rows()[0].get::<String>("plan")?)
    };

    let before = explain(&db).await?;

    db.schema()
        .label("Person")
        .index("age", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await?;
    let after_apply = explain(&db).await?;

    // Eliminate "registered but never built over the pre-existing rows" as the
    // explanation, rather than assuming it away.
    let rebuilt = db.indexes().rebuild("Person", false).await?;
    let after_rebuild = explain(&db).await?;

    println!("\n[probe:index-used] {Q}");
    println!("  rebuild('Person', background=false) -> {rebuilt:?}");
    println!(
        "  rebuild_status: {:?}",
        db.indexes().rebuild_status().await?
    );
    println!("  registered: {:?}", db.indexes().list(Some("Person")));
    println!("  plan:\n{before}");

    assert_eq!(
        before, after_apply,
        "the logical plan now changes when an index is created. That is a real \
         change in behaviour, not a new witness — index selection belongs to the \
         physical planner, and `index_scans` is the execution-side report. \
         Update this test deliberately rather than deleting the assertion."
    );
    assert_eq!(
        before, after_rebuild,
        "the logical plan changed after an explicit index rebuild — see above."
    );

    // The execution-side observable, on the shape this query cannot reach: a
    // range predicate collects no hash-index column, so no filter is pushed and
    // no index is consulted. `scans_reported` is asserted alongside so the zero
    // means "a scan ran and used no index" rather than "nothing was measured".
    let (w, rows) = probe(&db.session(), Q).await?;
    report("rebuilt", "node-filtered-int", &w, rows);
    assert_eq!(
        w.index_scans, 0,
        "a range predicate consulted an index; if a BTree route was added, the \
         index lever's query shapes need revisiting"
    );
    Ok(())
}

/// **Is the compaction transition even happening?** Now answerable.
///
/// This asked a question the old `CompactionStats` could not answer: five
/// deliberately separate flushes reported `files_compacted: 1, bytes_before: 0,
/// bytes_after: 0`, and nothing distinguished "the fragments were already
/// merged" from "compact did nothing" from "the struct does not report what its
/// field names suggest". It was the third.
///
/// With #172 fixed the answer is measurable, so this asserts it: every round
/// merges, and a re-compact with no writes in between merges nothing. That pair
/// is the real-vs-no-op distinction a compaction lever would need at run level.
///
/// The distinction is deterministic rather than timing-dependent. Lance treats
/// any fragment under `target_rows_per_fragment` (1 M by default) as a
/// compaction candidate, but skips a bin holding exactly one such fragment — so
/// two small fragments always merge, and the single fragment left behind always
/// plans empty.
#[tokio::test(flavor = "multi_thread")]
async fn probe_compaction_actually_compacts() -> anyhow::Result<()> {
    // Background compaction off, and every write below is an append: a delete
    // would leave a deletion file, which makes a lone fragment compact *itself*
    // and destroys the no-op half of this test.
    let db = build_dqp_seed_tuned(Fixture::TINY, 0, |c| c.compaction.enabled = false).await?;

    println!("\n[probe:compaction-real] one compaction per added fragment");
    let mut grand_total = 0;
    for round in 0..5 {
        insert_l0_delta(&db, 100).await?;
        db.flush().await?;
        // To a fixpoint, not one pass — see `compact_to_fixpoint` for why, and
        // for the measurement that established it.
        let (removed, final_pass) = compact_to_fixpoint(&db, "Person").await?;
        println!("  round {round}: merged {removed} fragments, settled at {final_pass:?}");
        assert!(
            removed >= 2,
            "round {round} merged nothing across every pass. The flush added a \
             fragment to the one left by the previous round, so there was a pair \
             to merge. removed={removed}, final={final_pass:?}"
        );
        grand_total += removed;
    }
    assert!(
        grand_total >= 10,
        "five rounds that each merge a pair must remove at least ten fragments; \
         got {grand_total}"
    );

    // And once more with everything already compacted, so a no-op has something
    // to be compared against. Converge first: one pass need not merge every bin.
    let again = compact_until_converged(&db, "Person").await?;
    println!("  converged re-compact (expected no-op): {again:?}");
    assert_eq!(
        (
            again.fragments_removed,
            again.fragments_added,
            again.files_removed,
            again.files_added
        ),
        (0, 0, 0, 0),
        "the no-op re-compact reported work, so the fields cannot distinguish a \
         real compaction from a no-op: {again:?}"
    );
    assert!(
        again.duration > std::time::Duration::ZERO,
        "duration is real even for a no-op — it measures the call, not the merge"
    );
    Ok(())
}
