//! Phase 0A — DQP feasibility measurement.
//!
//! This module measures; it does not assert a law. Its three tests answer the
//! questions the DQP oracle is about to be designed around, each of which the
//! proposal previously answered by assumption:
//!
//! 1. [`dqp_feasibility_measure_tiers`] — what does a case actually cost at each
//!    fixture size, and how many rows does it return? Sets the tier case counts
//!    and the row-budget ceilings.
//! 2. [`dqp_feasibility_probe_runtime_flavor`] — can a current-thread runtime
//!    (which `metamorphic::drive` uses) flush, fork and pin at all, or must the
//!    DQP drivers be multi-threaded?
//! 3. [`dqp_feasibility_audit_witness_observability`] — which `QueryMetrics`
//!    fields actually move, and which are declared-but-always-zero?
//!
//! All three are `#[ignore]`d and write machine-readable output under
//! `target/dqp-feasibility/`, so `docs/perf/dqp-feasibility-*.md` is regenerated
//! from data rather than transcribed by hand.
//!
//! Run with:
//!
//! ```text
//! cargo nextest run --profile soak -p uni-db --test integration \
//!   --run-ignored ignored-only -E 'test(/metamorphic::dqp::feasibility/)'
//! ```
//!
//! The `soak` profile is not optional: nextest's default profile kills a test at
//! 180 s (`.config/nextest.toml:10`) and the large tier's build alone is
//! expected to exceed that.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use uni_db::Uni;

use super::seed::{Tier, build_dqp_seed};
use crate::querygen::arb_case;
use crate::querygen::render::render;

/// Cases run per tier. Small on purpose — this is a cost probe, not a soak.
const PROBE_CASES: usize = 100;

/// Directory for machine-readable measurement output.
///
/// Anchored to the workspace `target/`, not the process CWD: nextest runs the
/// test binary with CWD set to the *crate* root, so a relative `target/…` would
/// silently create a stray `crates/uni/target/`.
fn out_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/dqp-feasibility")
}

/// Writes `body` to `out_dir()/name`, and echoes it so the run is legible under
/// `--nocapture` even if the artifact is later lost.
fn publish(name: &str, body: &str) {
    eprintln!("\n===== {name} =====\n{body}");
    let dir = out_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("warning: could not create {}: {e}", dir.display());
        return;
    }
    let path = dir.join(name);
    if let Err(e) = std::fs::write(&path, body) {
        eprintln!("warning: could not write {}: {e}", path.display());
    }
}

/// Nearest-rank percentile of an already-sorted slice.
fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = (((sorted.len() as f64) * p).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

/// Same, for row counts.
fn pct_usize(sorted: &[usize], p: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (((sorted.len() as f64) * p).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

/// Generates `n` deterministic cases, rendered to Cypher.
///
/// Uses proptest's strategy machinery directly rather than a `TestRunner::run`
/// loop: this is a measurement, so there is no law to shrink against and the
/// cases must be identical across tiers for the comparison to mean anything.
fn generate_cases(n: usize) -> Vec<String> {
    let mut runner = TestRunner::deterministic();
    let strategy = arb_case();
    (0..n)
        .map(|_| {
            let tree = strategy
                .new_tree(&mut runner)
                .expect("querygen strategy produced no value");
            render(&tree.current().base_query())
        })
        .collect()
}

/// Per-tier cost profile.
struct TierReport {
    tier: Tier,
    build: Duration,
    times: Vec<Duration>,
    rows: Vec<usize>,
    total_rows: usize,
    failures: usize,
}

async fn measure_tier(tier: Tier, cases: &[String]) -> anyhow::Result<TierReport> {
    let t0 = Instant::now();
    let db = build_dqp_seed(tier).await?;
    let build = t0.elapsed();
    eprintln!("[{}] fixture built in {build:?}", tier.name());

    let session = db.session();
    let mut times = Vec::with_capacity(cases.len());
    let mut rows = Vec::with_capacity(cases.len());
    let mut total_rows = 0usize;
    let mut failures = 0usize;

    for (i, cypher) in cases.iter().enumerate() {
        let t = Instant::now();
        match session.query(cypher).await {
            Ok(r) => {
                times.push(t.elapsed());
                let n = r.metrics().rows_returned;
                rows.push(n);
                total_rows += n;
            }
            Err(e) => {
                // A generator/engine disagreement is data, not a reason to abort
                // the measurement — but it must be counted, not swallowed.
                failures += 1;
                eprintln!("[{}] case {i} failed: {e}\n  query: {cypher}", tier.name());
            }
        }
    }

    times.sort_unstable();
    rows.sort_unstable();
    Ok(TierReport {
        tier,
        build,
        times,
        rows,
        total_rows,
        failures,
    })
}

/// Measures per-case cost and rows returned at every tier.
///
/// **The row budget is defined over rows *returned*, not rows *scanned*.** The
/// proposal specified it over `rows_scanned`, which is a declared-but-unpopulated
/// field (`uni-query/src/types.rs:36`) that always reads 0 — a budget over it
/// would never fire. Redefining the budget onto a metric that is genuinely
/// populated is exactly the kind of correction this phase exists to make.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "soak: Phase 0A feasibility measurement, builds fixtures up to 50k vertices"]
async fn dqp_feasibility_measure_tiers() -> anyhow::Result<()> {
    let cases = generate_cases(PROBE_CASES);
    let mut out = String::new();
    writeln!(out, "# DQP tier cost measurement\n")?;
    writeln!(out, "cases per tier: {PROBE_CASES}\n")?;
    writeln!(
        out,
        "| tier | persons | edges | build | p50 | p95 | max | rows p50 | rows p95 | rows max | total rows | failures |"
    )?;
    writeln!(out, "|---|---|---|---|---|---|---|---|---|---|---|---|")?;

    for tier in Tier::ALL {
        let r = measure_tier(tier, &cases).await?;
        writeln!(
            out,
            "| {} | {} | {} | {:?} | {:?} | {:?} | {:?} | {} | {} | {} | {} | {} |",
            r.tier.name(),
            r.tier.persons(),
            r.tier.edges(),
            r.build,
            pct(&r.times, 0.50),
            pct(&r.times, 0.95),
            r.times.last().copied().unwrap_or_default(),
            pct_usize(&r.rows, 0.50),
            pct_usize(&r.rows, 0.95),
            r.rows.last().copied().unwrap_or_default(),
            r.total_rows,
            r.failures,
        )?;

        // Extrapolation is the whole point: a tier is only viable if its
        // measured per-case cost times its intended case count fits the lane.
        let p95 = pct(&r.times, 0.95);
        for target in [500u32, 50_000u32] {
            let projected = p95.mul_f64(f64::from(target) * 2.0); // two sides
            eprintln!(
                "[{}] projected wall-clock for {target} cases x 2 sides at p95: {projected:?}",
                r.tier.name()
            );
        }
    }

    publish("tiers.md", &out);
    Ok(())
}

/// Probes whether a **current-thread** runtime can perform the operations the
/// Tier-1 and Tier-2 levers need.
///
/// `metamorphic::drive` builds a current-thread runtime (`metamorphic/mod.rs:70-73`),
/// which is sound for the existing read-only oracles. DQP is not read-only
/// during setup: creating a fork flushes L0 and runs a 2PC across the registry,
/// the allocator and one Lance branch per dataset, and pinning needs a snapshot
/// that also flushes. Those paths involve background tasks, and the fork TTL
/// suites already require the multi-thread flavor for the sweeper to progress.
///
/// A plain `#[test]` rather than `#[tokio::test]`, because the point is to build
/// the runtimes explicitly and compare them.
#[test]
#[ignore = "soak: Phase 0A runtime-flavor probe"]
fn dqp_feasibility_probe_runtime_flavor() {
    /// Generous enough that a slow-but-working op is not misreported as a hang.
    const OP_TIMEOUT: Duration = Duration::from_secs(60);

    fn probe(flavor: &str, multi: bool) -> String {
        let rt = if multi {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
        } else {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
        }
        .expect("runtime");

        let mut out = String::new();
        let result = rt.block_on(async {
            let db = build_dqp_seed(Tier::Tiny).await?;

            let mut row = |op: &str, r: Result<anyhow::Result<()>, tokio::time::error::Elapsed>| {
                let verdict = match r {
                    Ok(Ok(())) => "ok".to_string(),
                    Ok(Err(e)) => format!("error: {e}"),
                    Err(_) => "TIMED OUT".to_string(),
                };
                let _ = writeln!(out, "| {flavor} | {op} | {verdict} |");
            };

            let session = db.session();
            let r = tokio::time::timeout(OP_TIMEOUT, async {
                session.flush().await.map_err(Into::into)
            })
            .await;
            row("flush", r);

            let r = tokio::time::timeout(OP_TIMEOUT, async {
                session
                    .fork("dqp_probe")
                    .await
                    .map(|_| ())
                    .map_err(Into::into)
            })
            .await;
            row("fork", r);

            let r = tokio::time::timeout(OP_TIMEOUT, async {
                let snap = db.create_snapshot("dqp_probe_snap").await?;
                let mut s = db.session();
                s.pin_to_version(&snap).await?;
                anyhow::Ok(())
            })
            .await;
            row("snapshot + pin", r);

            anyhow::Ok(())
        });
        if let Err(e) = result {
            let _ = writeln!(out, "| {flavor} | fixture build | FAILED: {e} |");
        }
        out
    }

    let mut out = String::new();
    let _ = writeln!(out, "# Runtime flavor probe\n");
    let _ = writeln!(out, "| flavor | op | result |");
    let _ = writeln!(out, "|---|---|---|");
    out.push_str(&probe("current_thread", false));
    out.push_str(&probe("multi_thread", true));
    publish("runtime-flavor.md", &out);
}

/// Asserts which `QueryMetrics` fields are genuinely populated.
///
/// **This began as a Phase 0A report and is now a Phase 1 regression test.** It
/// measured that `rows_scanned`, `bytes_read`, `l0_reads`, `storage_reads` and
/// `cache_hits` all read zero on every path, despite existing as public fields
/// documented merely as "0 until … instrumentation". A field that exists and
/// always returns zero is **more** dangerous than a missing one: an activation
/// witness written against it compiles, runs, and silently never fires — and
/// written as `== 0` on side B it passes vacuously on *both* sides, which is
/// precisely the failure DQP exists to catch.
///
/// Phase 1 populated the three that back a witness. The other two stay zero, and
/// that is asserted rather than ignored — so "still a placeholder" is a stated
/// fact rather than something a future reader has to rediscover.
///
/// Non-ignored and cheap: this is the guard that keeps the trap shut.
#[tokio::test(flavor = "multi_thread")]
async fn dqp_audit_witness_observability() -> anyhow::Result<()> {
    let db = build_dqp_seed(Tier::Tiny).await?;
    let q = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS c0";

    // One session held across all three probes: the plan cache is session-local
    // (`session.rs:220-229`), so a fresh `db.session()` per query — which
    // `metamorphic::run_bag` does — can never observe a warm cache.
    let session = db.session();

    let after_flush = session.query(q).await?.metrics().clone();
    let repeat = session.query(q).await?.metrics().clone();

    // Now dirty L0 without flushing, so a subsequent read must merge L0 over L1.
    {
        let tx = session.tx().await?;
        for i in 0..64 {
            tx.execute(&format!(
                "CREATE (:Person {{name: 'extra{i}', age: 40, score: 0.5, city: 'NYC'}})"
            ))
            .await?;
        }
        tx.commit().await?;
    }
    let with_l0 = session.query(q).await?.metrics().clone();

    let mut out = String::new();
    writeln!(out, "# QueryMetrics observability audit\n")?;
    writeln!(
        out,
        "| field | L1 only | repeat (cache) | L0 over L1 | verdict |"
    )?;
    writeln!(out, "|---|---|---|---|---|")?;

    macro_rules! row {
        ($field:ident) => {{
            let a = after_flush.$field;
            let b = repeat.$field;
            let c = with_l0.$field;
            let observable = a != 0 || b != 0 || c != 0;
            writeln!(
                out,
                "| `{}` | {a} | {b} | {c} | {} |",
                stringify!($field),
                if observable {
                    "observable"
                } else {
                    "**ALWAYS ZERO**"
                }
            )?;
            observable
        }};
    }

    let rows_returned = row!(rows_returned);
    let rows_scanned = row!(rows_scanned);
    let bytes_read = row!(bytes_read);
    let l0_reads = row!(l0_reads);
    let storage_reads = row!(storage_reads);
    let cache_hits = row!(cache_hits);

    writeln!(
        out,
        "| `plan_cache_hit` | {} | {} | {} | {} |",
        after_flush.plan_cache_hit,
        repeat.plan_cache_hit,
        with_l0.plan_cache_hit,
        if repeat.plan_cache_hit {
            "observable (warm on repeat)"
        } else {
            "**NEVER WARM**"
        }
    )?;

    // `l1_run_count` is the compaction witness and lives on DatabaseMetrics, not
    // QueryMetrics — recorded here so the audit covers all six witnesses.
    let dbm = db.metrics();
    writeln!(
        out,
        "\nDatabaseMetrics::l1_run_count = {}",
        dbm.l1_run_count
    )?;

    writeln!(out, "\n## Phase 1 work list\n")?;
    for (name, ok) in [
        ("rows_returned", rows_returned),
        ("rows_scanned", rows_scanned),
        ("bytes_read", bytes_read),
        ("l0_reads", l0_reads),
        ("storage_reads", storage_reads),
        ("cache_hits", cache_hits),
    ] {
        if !ok {
            writeln!(
                out,
                "- `{name}` needs instrumentation before any witness may depend on it"
            )?;
        }
    }

    publish("witness-audit.md", &out);

    // ── the assertions this test exists for ─────────────────────────────────
    //
    // Positive: the counters that back a DQP activation witness must move.
    assert!(
        rows_returned,
        "`rows_returned` must be populated on the read path"
    );
    assert!(
        rows_scanned,
        "`rows_scanned` must be populated — the row budget is enforced over it"
    );
    assert!(
        storage_reads,
        "`storage_reads` must be populated — half the L0-vs-L1 witness"
    );
    assert!(
        with_l0.l0_reads > 0,
        "`l0_reads` must be non-zero once unflushed rows exist; got {} (the \
         L0-vs-L1 lever is unimplementable without it)",
        with_l0.l0_reads
    );
    assert_eq!(
        after_flush.l0_reads, 0,
        "`l0_reads` must be zero on a fully flushed read, or it is not \
         distinguishing the tiers at all"
    );

    // Negative: the two that back no witness stay zero *by decision*. If either
    // starts moving, this fires and whoever wired it can promote it here rather
    // than leaving a half-populated field for the next reader to misjudge.
    assert!(
        !bytes_read,
        "`bytes_read` is documented as unpopulated; wiring it means updating \
         its doc comment and this assertion together"
    );
    assert!(
        !cache_hits,
        "`cache_hits` is documented as unpopulated; see `bytes_read` above"
    );

    // The read path still does not set `plan_cache_hit` — it is a write-path
    // field. Pinned here so a change to that becomes a deliberate decision.
    assert!(
        !repeat.plan_cache_hit,
        "`plan_cache_hit` is set only on the transaction path; the read-path \
         plan-cache witness is the `SessionMetrics::plan_cache_hits` delta"
    );
    Ok(())
}

/// Characterizes where a plan-cache hit is actually observable on the **read**
/// path, which is the only path a read-only differential oracle uses.
///
/// This started as a guard asserting `QueryResult.metrics().plan_cache_hit`
/// becomes `true` on a repeated query — the witness the proposal's §3.3 table
/// names. It failed, and the cause is not a test bug:
///
/// **`plan_cache_hit` is populated only on the transaction/write path.** The
/// single assignment site is `execute_internal_with_tx_l0`
/// (`api/impl_query.rs:808`, surfaced at :920); the read path never sets it, so
/// on `Session::query` it is permanently `false`. The existing coverage
/// (`cypher_write/tx_plan_cache_test.rs`) is a *write*-path test, which is why
/// the gap went unnoticed.
///
/// The read-path plan cache is real — it is the subject of that same file's
/// stale-plan regression — but its hits are counted on
/// `SessionMetrics::plan_cache_hits` (`session.rs:1276`), a per-session
/// cumulative counter rather than a per-query flag. So the plan-cache lever's
/// witness must be a **delta on `Session::metrics()` across the two sides**, not
/// the `QueryResult` field.
///
/// Non-ignored: cheap, and it pins a fact the oracle design depends on.
#[tokio::test(flavor = "multi_thread")]
async fn plan_cache_hit_is_observable_only_via_session_metrics_on_the_read_path()
-> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Person")
        .property("name", uni_db::DataType::String)
        .done()
        .apply()
        .await?;
    let session = db.session();
    let q = "MATCH (p:Person) RETURN p.name AS c0";

    let before = session.metrics().plan_cache_hits;
    let first = session.query(q).await?;
    let after_first = session.metrics().plan_cache_hits;
    let second = session.query(q).await?;
    let after_second = session.metrics().plan_cache_hits;

    // The QueryResult flag is dead on this path — asserted so a future change
    // that starts populating it breaks this test loudly rather than silently
    // changing what the DQP witness means.
    assert!(
        !first.metrics().plan_cache_hit && !second.metrics().plan_cache_hit,
        "QueryResult.plan_cache_hit is set only by execute_internal_with_tx_l0 \
         (impl_query.rs:808), so the read path must report false on both calls; \
         if this now fires, the plan-cache witness can move back to QueryResult"
    );

    assert_eq!(
        after_first, before,
        "the first query on a fresh session must miss the read-plan cache"
    );
    assert!(
        after_second > after_first,
        "the second identical query on the SAME session must register a \
         read-plan-cache hit on SessionMetrics (before={before}, \
         after_first={after_first}, after_second={after_second}); if this fails, \
         the plan-cache DQP lever has no observable witness at all"
    );

    // The converse, and the reason `run_bag` cannot host this lever: it calls
    // `db.session()` per query (`metamorphic/mod.rs:115-119`) and every session
    // gets a fresh plan cache (`session.rs:220-229`), so a per-query session can
    // never observe a warm cache.
    let cold_session = db.session();
    cold_session.query(q).await?;
    assert_eq!(
        cold_session.metrics().plan_cache_hits,
        0,
        "a fresh session must start cold"
    );
    Ok(())
}
