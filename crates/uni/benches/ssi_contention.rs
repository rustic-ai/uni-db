// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Contention curves: throughput **and abort rate** vs contention.
//!
//! A single number hides the shape change from "aborts rise gracefully" to
//! "aborts collapse throughput", so this sweeps two axes and reports both
//! quantities per cell:
//!
//! * **skew** — a Zipf parameter `theta` over a keyspace of [`KEYS`] counters.
//!   `theta = 0` is uniform; higher values concentrate writes onto the head of
//!   the distribution. The previous shape of this bench had a **single hot
//!   key**, which is `theta = infinity` — the one point on the axis where the
//!   curve is invisible.
//! * **writers** — concurrent tasks, each doing a fixed number of
//!   read-modify-write increments.
//!
//! Both SSI arms run as cells of one sweep rather than as two invocations, so
//! the gap appears in one report.
//!
//! # The off arm is a reference line, not an alternative
//!
//! With `ssi_enabled = false` the engine reproduces last-writer-wins: no
//! validation runs, so no transaction ever aborts and **increments are silently
//! lost**. Its throughput is the ceiling that correctness is purchased against;
//! it is not a mode anyone should run. Correctness of the on arm (final ==
//! total increments) is covered by the stress tests, not here.
//!
//! # Abort rate
//!
//! `uni-store`'s commit path emits one `uni_ssi_commit_validations_total` per
//! writing commit and one `uni_ssi_serialization_conflicts_total` per aborted
//! one. The comment beside the emission at
//! `crates/uni-store/src/runtime/writer.rs` states the intent directly: *"the
//! ratio of conflicts to validations is the headline abort rate."* This bench
//! is the first thing to compute it.
//!
//! Counters are read through the same in-process probe the SSI tests use,
//! included directly rather than reimplemented — `metrics::set_global_recorder`
//! may be installed at most once per process and, as of metrics-util 0.20,
//! `Snapshotter::snapshot()` *consumes* counters. The probe already solves both;
//! a second implementation would step on them again.
//!
//! # Running
//!
//! ```bash
//! cargo bench -p uni-db --bench ssi_contention
//!
//! # Trim the sweep (comma-separated).
//! CONTENTION_THETAS=0,1.2 CONTENTION_WRITERS=1,24 cargo bench -p uni-db --bench ssi_contention
//! ```
//!
//! The markdown table it prints is the deliverable; it goes to `docs/perf/`.

// Lance 7's `lance_io::uring` moka cache introduces a deeply nested type whose
// `Send` auto-trait evaluation exceeds the default recursion limit inside the
// `tokio::spawn` futures below. Raise the limit per the compiler's suggestion.
#![recursion_limit = "256"]

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion};
use mimalloc::MiMalloc;
use tokio::runtime::Runtime;
use uni_db::{Uni, UniConfig};

// The counter probe lives with the SSI tests. Including it keeps one
// implementation of the install-once + consuming-snapshotter handling; benches
// receive dev-dependencies, so `metrics-util` resolves here too.
#[path = "../tests/common/ssi_support/metrics.rs"]
#[allow(dead_code)]
mod counters;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Counters in the keyspace. Large enough that `theta = 0` genuinely spreads
/// writes (so the abort rate has somewhere to fall to), small enough that a high
/// theta concentrates hard.
const KEYS: usize = 256;
/// Increments per writer per iteration.
const INCREMENTS_PER_WRITER: usize = 10;

const DEFAULT_THETAS: &[f64] = &[0.0, 0.9, 1.2];
const DEFAULT_WRITERS: &[usize] = &[1, 8, 24];

/// One measured cell of the sweep.
#[derive(Clone)]
struct Row {
    ssi: bool,
    theta: f64,
    writers: usize,
    elapsed: Duration,
    ops: u64,
    validations: u64,
    conflicts: u64,
    exhausted: u64,
}

fn rows() -> &'static Mutex<Vec<Row>> {
    static ROWS: OnceLock<Mutex<Vec<Row>>> = OnceLock::new();
    ROWS.get_or_init(|| Mutex::new(Vec::new()))
}

// --------------------------------------------------------------------------
// Zipf
// --------------------------------------------------------------------------

/// Deterministic Zipf sampler over `0..n`.
///
/// Precomputes the normalized CDF and samples by binary search. Deterministic
/// because a contention *shape* that moved between runs could not be compared
/// across a code change.
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize, theta: f64) -> Self {
        let weights: Vec<f64> = (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).collect();
        let total: f64 = weights.iter().sum();
        let mut acc = 0.0;
        let cdf = weights
            .iter()
            .map(|w| {
                acc += w / total;
                acc
            })
            .collect();
        Self { cdf }
    }

    fn sample(&self, u: f64) -> usize {
        match self
            .cdf
            .binary_search_by(|p| p.partial_cmp(&u).unwrap_or(std::cmp::Ordering::Equal))
        {
            Ok(i) => i,
            Err(i) => i.min(self.cdf.len() - 1),
        }
    }
}

/// xorshift64*, matching the deterministic-PRNG convention the other benches use.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// Uniform in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// --------------------------------------------------------------------------
// workload
// --------------------------------------------------------------------------

async fn fresh_counters(ssi: bool) -> Arc<Uni> {
    let config = UniConfig {
        ssi_enabled: ssi,
        ..Default::default()
    };
    let db = Uni::in_memory().config(config).build().await.unwrap();
    db.schema()
        .label("Counter")
        .property("id", uni_db::DataType::String)
        .property("n", uni_db::DataType::Int)
        .done()
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    // One statement rather than KEYS of them: seeding runs once per criterion
    // iteration and is not the thing being measured, so its cost is pure
    // overhead on every cell.
    let patterns: Vec<String> = (0..KEYS)
        .map(|k| format!("(:Counter {{id: 'k{k}', n: 0}})"))
        .collect();
    tx.execute(&format!("CREATE {}", patterns.join(", ")))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    Arc::new(db)
}

/// Runs `writers` tasks of `INCREMENTS_PER_WRITER` Zipf-targeted increments.
/// Returns how many operations exhausted their retry budget.
async fn run_contended(db: Arc<Uni>, writers: usize, theta: f64, seed: u64) -> u64 {
    let zipf = Arc::new(Zipf::new(KEYS, theta));
    let mut handles = Vec::with_capacity(writers);
    for w in 0..writers {
        let db = db.clone();
        let zipf = zipf.clone();
        // Each writer draws from its own stream, so the sweep is reproducible
        // but writers do not march in lockstep.
        let mut rng = Rng(seed ^ ((w as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)));
        handles.push(tokio::spawn(async move {
            let mut exhausted = 0u64;
            // One session per writer, hoisted out of the loop. `Session` is
            // Clone and carries the plan cache, so a fresh one per operation
            // would re-plan every statement and bury the conflict cost this
            // bench exists to measure under planning overhead.
            let session = db.session();
            for _ in 0..INCREMENTS_PER_WRITER {
                let k = zipf.sample(rng.unit());
                // A conflict that outlives the retry budget is a real outcome
                // under heavy contention, not a reason to abort the bench --
                // it is part of the shape being measured.
                if session
                    .execute_with_retry(&format!(
                        "MATCH (c:Counter {{id: 'k{k}'}}) SET c.n = c.n + 1"
                    ))
                    .await
                    .is_err()
                {
                    exhausted += 1;
                }
            }
            exhausted
        }));
    }
    let mut exhausted = 0;
    for h in handles {
        exhausted += h.await.unwrap();
    }
    exhausted
}

// --------------------------------------------------------------------------
// sweep
// --------------------------------------------------------------------------

fn env_list<T>(var: &str, default: &[T]) -> Vec<T>
where
    T: std::str::FromStr + Copy,
{
    match std::env::var(var) {
        Ok(s) => s
            .split(',')
            .filter(|p| !p.trim().is_empty())
            .map(|p| {
                p.trim()
                    .parse()
                    .unwrap_or_else(|_| panic!("{var}: cannot parse {p:?}"))
            })
            .collect(),
        Err(_) => default.to_vec(),
    }
}

fn env_scalar(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .map(|s| {
            s.trim()
                .parse()
                .unwrap_or_else(|_| panic!("{var}: cannot parse {s:?}"))
        })
        .unwrap_or(default)
}

fn bench_contention(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let thetas = env_list("CONTENTION_THETAS", DEFAULT_THETAS);
    let writers_sweep = env_list("CONTENTION_WRITERS", DEFAULT_WRITERS);

    let mut group = c.benchmark_group("ssi_contention");
    // Criterion's own --sample-size / --measurement-time are overridden by these
    // group settings, so they get their own env knobs. A nightly lane needs the
    // full-cost sweep; a local smoke needs to prove the plumbing in seconds.
    group.sample_size(env_scalar("CONTENTION_SAMPLES", 10));
    group.measurement_time(Duration::from_secs(
        env_scalar("CONTENTION_MEASURE_SECS", 10) as u64,
    ));
    group.warm_up_time(Duration::from_secs(
        env_scalar("CONTENTION_WARMUP_SECS", 3) as u64
    ));

    for &ssi in &[true, false] {
        for &theta in &thetas {
            for &w in &writers_sweep {
                let arm = if ssi { "ssi" } else { "lww" };
                let id = format!("{arm}/theta{theta}/w{w}");
                group.bench_with_input(BenchmarkId::from_parameter(&id), &w, |b, &w| {
                    b.iter_custom(|iters| {
                        rt.block_on(async move {
                            let mut total = Duration::ZERO;
                            let mut ops = 0u64;
                            let mut exhausted = 0u64;
                            let mut validations = 0u64;
                            let mut conflicts = 0u64;
                            for i in 0..iters {
                                let db = fresh_counters(ssi).await;
                                // Probes bracket the contended region only. The
                                // seeding CREATE is itself a writing commit, so
                                // starting them before it would put setup in the
                                // abort-rate denominator and dilute every cell.
                                let v = counters::CounterProbe::start(
                                    "uni_ssi_commit_validations_total",
                                    &[],
                                );
                                let cf = counters::CounterProbe::start(
                                    "uni_ssi_serialization_conflicts_total",
                                    &[],
                                );
                                let start = Instant::now();
                                exhausted += run_contended(db, w, theta, 0xC0FF_EE00 ^ i).await;
                                total += start.elapsed();
                                validations += v.delta();
                                conflicts += cf.delta();
                                ops += (w * INCREMENTS_PER_WRITER) as u64;
                            }
                            rows().lock().unwrap().push(Row {
                                ssi,
                                theta,
                                writers: w,
                                elapsed: total,
                                ops,
                                validations,
                                conflicts,
                                exhausted,
                            });
                            total
                        })
                    });
                });
            }
        }
    }

    group.finish();
}

// --------------------------------------------------------------------------
// report
// --------------------------------------------------------------------------

struct Agg {
    ops: u64,
    secs: f64,
    validations: u64,
    conflicts: u64,
    exhausted: u64,
}

impl Agg {
    fn throughput(&self) -> f64 {
        if self.secs > 0.0 {
            self.ops as f64 / self.secs
        } else {
            0.0
        }
    }
    fn abort_pct(&self) -> f64 {
        if self.validations > 0 {
            100.0 * self.conflicts as f64 / self.validations as f64
        } else {
            0.0
        }
    }
}

/// Fold every criterion sample of a cell into one aggregate.
fn aggregate() -> Vec<((bool, String, usize), Agg)> {
    let mut out: Vec<((bool, String, usize), Agg)> = Vec::new();
    for r in rows().lock().unwrap().iter() {
        let key = (r.ssi, format!("{:.2}", r.theta), r.writers);
        match out.iter_mut().find(|(k, _)| *k == key) {
            Some((_, a)) => {
                a.ops += r.ops;
                a.secs += r.elapsed.as_secs_f64();
                a.validations += r.validations;
                a.conflicts += r.conflicts;
                a.exhausted += r.exhausted;
            }
            None => out.push((
                key,
                Agg {
                    ops: r.ops,
                    secs: r.elapsed.as_secs_f64(),
                    validations: r.validations,
                    conflicts: r.conflicts,
                    exhausted: r.exhausted,
                },
            )),
        }
    }
    out.sort_by(|(a, _), (b, _)| b.0.cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    out
}

/// Print the deliverable, then refuse to call a sweep that did not contend a
/// result.
///
/// This is B4's analogue of C1's per-lever activation witnesses. A contention
/// curve whose abort rate is flat at zero across every theta is not a curve: it
/// means the workload never collided, and publishing its throughput numbers as
/// "under contention" would be decoration. The check fails the bench rather than
/// printing a table nobody can trust.
fn report() {
    let agg = aggregate();
    if agg.is_empty() {
        eprintln!("[contention] no cells were measured");
        std::process::exit(1);
    }

    println!("\n## Contention sweep\n");
    println!("keyspace = {KEYS} counters, {INCREMENTS_PER_WRITER} increments/writer\n");
    println!(
        "| arm | theta | writers | ops/s | abort rate | conflicts/validations | retry-exhausted |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for ((ssi, theta, w), a) in &agg {
        println!(
            "| {} | {} | {} | {:.0} | {:.2}% | {}/{} | {} |",
            if *ssi { "ssi" } else { "lww" },
            theta,
            w,
            a.throughput(),
            a.abort_pct(),
            a.conflicts,
            a.validations,
            a.exhausted,
        );
    }
    println!();

    // --- non-vacuity -------------------------------------------------------
    let ssi_cells: Vec<_> = agg.iter().filter(|((ssi, ..), _)| *ssi).collect();
    if ssi_cells.is_empty() {
        eprintln!("[contention] the ssi arm produced no cells");
        std::process::exit(1);
    }

    let validations: u64 = ssi_cells.iter().map(|(_, a)| a.validations).sum();
    if validations == 0 {
        eprintln!(
            "[contention] VACUOUS: zero commit validations across the whole ssi arm. Either the \
             counters are not being observed or no writing commit ran; the throughput numbers \
             describe nothing."
        );
        std::process::exit(1);
    }

    // The multi-writer cells are the ones that can collide at all: a single
    // writer has nobody to conflict with, so its abort rate is legitimately 0
    // and must not be counted as evidence either way.
    let contended: Vec<_> = ssi_cells.iter().filter(|((_, _, w), _)| *w > 1).collect();
    if contended.is_empty() {
        eprintln!(
            "[contention] the sweep has no multi-writer ssi cell, so it cannot observe an abort. \
             Widen CONTENTION_WRITERS."
        );
        std::process::exit(1);
    }
    let peak = contended
        .iter()
        .map(|(_, a)| a.abort_pct())
        .fold(0.0_f64, f64::max);
    if peak <= 0.0 {
        eprintln!(
            "[contention] VACUOUS: no multi-writer ssi cell recorded a single conflict across \
             {validations} validations. The sweep is not contending, so its curve is flat by \
             construction rather than by measurement."
        );
        std::process::exit(1);
    }

    // The lww arm must be the opposite: validation is skipped entirely when
    // `ssi_enabled = false`, so a conflict there would mean the arm is not
    // actually off and the two arms are the same measurement twice -- the exact
    // defect this bench was rewritten to fix.
    let lww_conflicts: u64 = agg
        .iter()
        .filter(|((ssi, ..), _)| !*ssi)
        .map(|(_, a)| a.conflicts)
        .sum();
    if lww_conflicts > 0 {
        eprintln!(
            "[contention] the lww arm recorded {lww_conflicts} serialization conflicts. With \
             ssi_enabled = false no validation runs, so this means the arm did not take effect."
        );
        std::process::exit(1);
    }

    println!(
        "[contention] non-vacuity: peak abort rate {peak:.2}% over {validations} validations in \
         the ssi arm; lww arm recorded 0 conflicts as expected."
    );
}

fn main() {
    let mut c = Criterion::default().configure_from_args();
    bench_contention(&mut c);
    c.final_summary();
    // Criterion's `--test` mode runs each cell once to check it does not panic.
    // The sample counts are then far too small to describe a curve, so the
    // report is skipped -- loudly, so a `--test` run is never mistaken for a
    // measurement.
    if std::env::args().any(|a| a == "--test") {
        eprintln!("[contention] --test mode: cells ran once for smoke only; no curve reported");
        return;
    }
    report();
}
