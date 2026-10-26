// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #311: inside a transaction, a read through a scalar index after a
//! `SET` that rewrites the indexed property must see each re-keyed node
//! exactly once, at its new value — and not at its old one.
//!
//! The matrix varies where the base rows live (flushed / unflushed / half
//! flushed), whether a flush lands while the transaction is open (what the
//! default 5 s auto-flush does intermittently), and the index (none — the
//! oracle — BTree, Hash). Every arm must report the same counts.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(issue_311)' --no-capture

// Rust guideline compliant

use std::collections::HashSet;

use anyhow::{Context, Result};
use uni_db::{DataType, IndexType, ScalarType, Uni, UniConfig, Value};

const N: i64 = 3000;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Base {
    Flushed,
    Unflushed,
    HalfFlushed,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Idx {
    None,
    BTree,
    Hash,
}

#[derive(Debug, PartialEq, Clone)]
struct Counts {
    set_rows: usize,
    new_rows: usize,
    new_distinct: usize,
    old_rows: usize,
    after_new_rows: usize,
    after_new_distinct: usize,
    after_old_rows: usize,
}

const EXPECTED: Counts = Counts {
    set_rows: N as usize,
    new_rows: N as usize,
    new_distinct: N as usize,
    old_rows: 0,
    after_new_rows: N as usize,
    after_new_distinct: N as usize,
    after_old_rows: 0,
};

fn vids(rows: &uni_db::QueryResult) -> (usize, usize) {
    let all: Vec<i64> = rows
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::Int(i) => *i,
            other => panic!("vid: {other:?}"),
        })
        .collect();
    let distinct: HashSet<i64> = all.iter().copied().collect();
    (all.len(), distinct.len())
}

async fn arm(base: Base, idx: Idx, mid_tx_flush: bool) -> Result<Counts> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            strict_schema: true,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("Concept")
        .property("sid", DataType::Int)
        .property("view", DataType::String)
        .apply()
        .await?;

    let load = |lo: i64, hi: i64| {
        let db = &db;
        async move {
            let s = db.session();
            let tx = s.tx().await?;
            tx.execute_with(
                "UNWIND range($lo, $hi - 1) AS i CREATE (:Concept {sid: i, view: 'old'})",
            )
            .param("lo", Value::Int(lo))
            .param("hi", Value::Int(hi))
            .run()
            .await?;
            tx.commit().await?;
            anyhow::Ok(())
        }
    };
    match base {
        Base::Flushed => {
            load(0, N).await?;
            db.flush().await?;
        }
        Base::Unflushed => load(0, N).await?,
        Base::HalfFlushed => {
            load(0, N / 2).await?;
            db.flush().await?;
            load(N / 2, N).await?;
        }
    }
    let it = match idx {
        Idx::None => None,
        Idx::BTree => Some(IndexType::Scalar(ScalarType::BTree)),
        Idx::Hash => Some(IndexType::Scalar(ScalarType::Hash)),
    };
    if let Some(it) = it {
        db.schema()
            .label("Concept")
            .index("view", it)
            .apply()
            .await?;
    }

    let s = db.session();
    let tx = s.tx().await?;
    let set = tx
        .query_with(
            "MATCH (n:Concept) WHERE n.view = $old SET n.view = $new \
             RETURN id(n) AS vid, n.sid AS sid",
        )
        .param("old", "old")
        .param("new", "new")
        .fetch_all()
        .await?;
    let (set_rows, _) = vids(&set);
    if mid_tx_flush {
        // Moves the committed base rows from L0 to L1 while the tx is open.
        db.flush().await?;
    }
    let q_new = "MATCH (n:Concept) WHERE n.view = $v RETURN id(n) AS vid, n.sid AS sid";
    let new = tx.query_with(q_new).param("v", "new").fetch_all().await?;
    let (new_rows, new_distinct) = vids(&new);
    let old = tx.query_with(q_new).param("v", "old").fetch_all().await?;
    let (old_rows, _) = vids(&old);
    tx.commit().await?;

    let after_new = s.query_with(q_new).param("v", "new").fetch_all().await?;
    let (after_new_rows, after_new_distinct) = vids(&after_new);
    let after_old = s.query_with(q_new).param("v", "old").fetch_all().await?;
    let (after_old_rows, _) = vids(&after_old);

    Ok(Counts {
        set_rows,
        new_rows,
        new_distinct,
        old_rows,
        after_new_rows,
        after_new_distinct,
        after_old_rows,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn issue_311_tx_index_rekey_matrix() -> Result<()> {
    let mut failures = Vec::new();
    for base in [Base::Flushed, Base::Unflushed, Base::HalfFlushed] {
        for mid in [false, true] {
            for idx in [Idx::None, Idx::BTree, Idx::Hash] {
                let got = arm(base, idx, mid).await?;
                let ok = got == EXPECTED;
                println!(
                    "{:<11?} mid_tx_flush={:<5} idx={:<5?} {} {:?}",
                    base,
                    mid,
                    idx,
                    if ok { "OK  " } else { "FAIL" },
                    got
                );
                if !ok {
                    failures.push((base, mid, idx, got));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "expected {EXPECTED:?} in every arm; failing arms: {failures:#?}"
    );
    Ok(())
}

/// Index-free oracle: every `(vid, view)` pair, counted in Rust.
async fn oracle_tx(tx: &uni_db::Transaction, v: &str) -> Result<(usize, usize)> {
    let all = tx
        .query("MATCH (n:Concept) RETURN id(n) AS vid, n.view AS view")
        .await?;
    let hits: Vec<i64> = all
        .rows()
        .iter()
        .filter(|r| matches!(&r.values()[1], Value::String(s) if s == v))
        .map(|r| match &r.values()[0] {
            Value::Int(i) => *i,
            other => panic!("vid: {other:?}"),
        })
        .collect();
    let distinct: HashSet<i64> = hits.iter().copied().collect();
    Ok((hits.len(), distinct.len()))
}

/// The concurrent shape of the report: background flushes land while the
/// re-key transaction is open and while its statements are running.
fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

async fn stress_once(idx: Idx, n: i64, round: usize) -> Result<Option<String>> {
    let auto_ms = knob("ISSUE_311_AUTO_MS", 20);
    let db = std::sync::Arc::new(
        Uni::in_memory()
            .config(UniConfig {
                auto_flush_threshold: knob("ISSUE_311_AUTO_THRESH", 1_000) as usize,
                auto_flush_interval: (auto_ms > 0)
                    .then(|| std::time::Duration::from_millis(auto_ms)),
                query_timeout: std::time::Duration::from_secs(knob("ISSUE_311_QTIMEOUT_S", 30)),
                strict_schema: true,
                ..Default::default()
            })
            .build()
            .await?,
    );
    db.schema()
        .label("Concept")
        .property("sid", DataType::Int)
        .property("view", DataType::String)
        .apply()
        .await?;
    if let Some(it) = match idx {
        Idx::None => None,
        Idx::BTree => Some(IndexType::Scalar(ScalarType::BTree)),
        Idx::Hash => Some(IndexType::Scalar(ScalarType::Hash)),
    } {
        db.schema()
            .label("Concept")
            .index("view", it)
            .apply()
            .await?;
    }
    // Load in several commits so some land flushed and some do not.
    for chunk in 0..4 {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute_with("UNWIND range($lo, $hi - 1) AS i CREATE (:Concept {sid: i, view: 'old'})")
            .param("lo", Value::Int(chunk * n / 4))
            .param("hi", Value::Int((chunk + 1) * n / 4))
            .run()
            .await?;
        tx.commit().await?;
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flusher_on = knob("ISSUE_311_FLUSHER", 1) == 1;
    let flusher = {
        let db = db.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            while flusher_on && !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = db.flush().await;
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
    };
    let s = db.session();
    let tx = s.tx().await?;
    let t0 = std::time::Instant::now();
    let set = tx
        .query_with(
            "MATCH (n:Concept) WHERE n.view = $old SET n.view = $new \
             RETURN id(n) AS vid, n.sid AS sid",
        )
        .param("old", "old")
        .param("new", "new")
        .timeout(std::time::Duration::from_secs(1800))
        .fetch_all()
        .await
        .context("SET statement")?;
    let set_ms = t0.elapsed().as_millis();
    let (set_rows, set_distinct) = vids(&set);
    let q = "MATCH (n:Concept) WHERE n.view = $v RETURN id(n) AS vid, n.sid AS sid";
    let new = tx
        .query_with(q)
        .param("v", "new")
        .fetch_all()
        .await
        .context("read new in tx")?;
    let new_idx_scans = new.metrics().index_scans;
    let (new_rows, new_distinct) = vids(&new);
    let (old_rows, _) = vids(&tx.query_with(q).param("v", "old").fetch_all().await?);
    let (orc_new, orc_new_d) = oracle_tx(&tx, "new").await?;
    tx.commit().await.context("commit")?;
    let after = s.query_with(q).param("v", "new").fetch_all().await?;
    let (after_rows, after_distinct) = vids(&after);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    flusher.await?;
    let nn = n as usize;
    let line = format!(
        "round={round} idx={idx:?} set={set_rows}/{set_distinct} new={new_rows}/{new_distinct} \
         old={old_rows} oracle_new={orc_new}/{orc_new_d} after={after_rows}/{after_distinct} \
         new_index_scans={new_idx_scans} set_ms={set_ms} \
         throttle_waits={} l1_runs={}",
        db.metrics().write_throttle_waits,
        db.metrics().l1_run_count,
    );
    let ok = set_rows == nn
        && set_distinct == nn
        && new_rows == nn
        && new_distinct == nn
        && old_rows == 0
        && orc_new == nn
        && after_rows == nn
        && after_distinct == nn;
    println!("{} {line}", if ok { "OK  " } else { "FAIL" });
    Ok((!ok).then_some(line))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow (~14 s): repeated rounds under a flush loop; run by the nightly ignored sweep"]
async fn issue_311_tx_index_rekey_concurrent_flush_stress() -> Result<()> {
    let rounds: usize = std::env::var("ISSUE_311_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6);
    let mut failures = Vec::new();
    for round in 0..rounds {
        for idx in [Idx::None, Idx::BTree] {
            if let Some(f) = stress_once(idx, knob("ISSUE_311_N", 4000) as i64, round).await? {
                failures.push(f);
            }
        }
    }
    assert!(failures.is_empty(), "failing rounds: {failures:#?}");
    Ok(())
}

/// Repeated re-keying across transactions — the incremental-view-maintenance
/// shape. Every cycle rewrites the indexed value again, so flushed storage
/// accumulates several MVCC versions per vid, each carrying a different key.
/// Flushes land after commit, mid-transaction, both, or neither, in rotation;
/// a non-indexed property update between cycles adds versions whose key did
/// not change. A noise group keeps a fixed key throughout.
async fn cycles_once(idx: Idx) -> Result<Vec<String>> {
    let n = knob("ISSUE_311_N", 3000) as i64;
    let noise = n / 3;
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            strict_schema: true,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("Concept")
        .property("sid", DataType::Int)
        .property("view", DataType::String)
        .property("payload", DataType::Int)
        .apply()
        .await?;
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute_with(
            "UNWIND range(0, $n - 1) AS i CREATE (:Concept {sid: i, view: 'v0', payload: 0})",
        )
        .param("n", Value::Int(n))
        .run()
        .await?;
        tx.execute_with(
            "UNWIND range(0, $m - 1) AS i \
             CREATE (:Concept {sid: -1 - i, view: 'noise', payload: 0})",
        )
        .param("m", Value::Int(noise))
        .run()
        .await?;
        tx.commit().await?;
    }
    db.flush().await?;
    if let Some(it) = match idx {
        Idx::None => None,
        Idx::BTree => Some(IndexType::Scalar(ScalarType::BTree)),
        Idx::Hash => Some(IndexType::Scalar(ScalarType::Hash)),
    } {
        db.schema()
            .label("Concept")
            .index("view", it)
            .apply()
            .await?;
    }
    let q = "MATCH (n:Concept) WHERE n.view = $v RETURN id(n) AS vid";
    let nn = n as usize;
    let mut failures = Vec::new();
    let cycles = knob("ISSUE_311_CYCLES", 12) as usize;
    for c in 1..=cycles {
        let (old, new) = (format!("v{}", c - 1), format!("v{c}"));
        let s = db.session();
        let tx = s.tx().await?;
        let set = tx
            .query_with(
                "MATCH (n:Concept) WHERE n.view = $old SET n.view = $new RETURN id(n) AS vid",
            )
            .param("old", old.as_str())
            .param("new", new.as_str())
            .fetch_all()
            .await?;
        if c % 4 == 2 || c % 4 == 3 {
            db.flush().await?;
        }
        let (set_rows, set_d) = vids(&set);
        let (in_new, in_new_d) = vids(
            &tx.query_with(q)
                .param("v", new.as_str())
                .fetch_all()
                .await?,
        );
        let (in_old, _) = vids(
            &tx.query_with(q)
                .param("v", old.as_str())
                .fetch_all()
                .await?,
        );
        let (in_noise, in_noise_d) = vids(&tx.query_with(q).param("v", "noise").fetch_all().await?);
        let (orc, orc_d) = oracle_tx(&tx, &new).await?;
        tx.commit().await?;
        if c % 4 == 1 || c % 4 == 3 {
            db.flush().await?;
        }
        let (af_new, af_new_d) = vids(&s.query_with(q).param("v", new.as_str()).fetch_all().await?);
        let (af_old, _) = vids(&s.query_with(q).param("v", old.as_str()).fetch_all().await?);
        // A version whose key does not change.
        if c % 3 == 0 {
            let tx = s.tx().await?;
            tx.execute_with("MATCH (n:Concept) WHERE n.sid % 2 = 0 SET n.payload = $c")
                .param("c", Value::Int(c as i64))
                .run()
                .await?;
            tx.commit().await?;
            if c % 2 == 0 {
                db.flush().await?;
            }
        }
        let line = format!(
            "idx={idx:?} cycle={c} set={set_rows}/{set_d} tx_new={in_new}/{in_new_d} \
             tx_old={in_old} tx_noise={in_noise}/{in_noise_d} oracle={orc}/{orc_d} \
             after_new={af_new}/{af_new_d} after_old={af_old}"
        );
        let ok = [
            set_rows, set_d, in_new, in_new_d, orc, orc_d, af_new, af_new_d,
        ]
        .iter()
        .all(|&x| x == nn)
            && in_old == 0
            && af_old == 0
            && in_noise == noise as usize
            && in_noise_d == noise as usize;
        println!("{} {line}", if ok { "OK  " } else { "FAIL" });
        if !ok {
            failures.push(line);
        }
    }
    Ok(failures)
}

/// Control: the index arms above only mean something if the `=` actually
/// reaches the index, both outside and inside a transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn issue_311_tx_index_rekey_index_is_consulted() -> Result<()> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            strict_schema: true,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("Concept")
        .property("sid", DataType::Int)
        .property("view", DataType::String)
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute_with("UNWIND range(0, $n - 1) AS i CREATE (:Concept {sid: i, view: 'old'})")
        .param("n", Value::Int(N))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;
    db.schema()
        .label("Concept")
        .index("view", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await?;
    let q = "MATCH (n:Concept) WHERE n.view = $v RETURN id(n) AS vid";
    let outside = s.query_with(q).param("v", "old").fetch_all().await?;
    let tx = s.tx().await?;
    let inside_before = tx.query_with(q).param("v", "old").fetch_all().await?;
    tx.query_with("MATCH (n:Concept) WHERE n.view = 'old' SET n.view = 'new' RETURN id(n)")
        .fetch_all()
        .await?;
    let inside_after_new = tx.query_with(q).param("v", "new").fetch_all().await?;
    let inside_after_old = tx.query_with(q).param("v", "old").fetch_all().await?;
    println!(
        "index_scans: outside={} (rows {}), in-tx before SET={} (rows {}), \
         in-tx after SET new={} (rows {}), old={} (rows {})",
        outside.metrics().index_scans,
        outside.rows().len(),
        inside_before.metrics().index_scans,
        inside_before.rows().len(),
        inside_after_new.metrics().index_scans,
        inside_after_new.rows().len(),
        inside_after_old.metrics().index_scans,
        inside_after_old.rows().len(),
    );
    tx.rollback();
    assert!(
        outside.metrics().index_scans >= 1,
        "control: index not consulted"
    );
    assert!(
        inside_after_old.metrics().index_scans >= 1,
        "in-tx lookup after SET did not consult the index"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow (~27 s): twelve re-key cycles per index; run by the nightly ignored sweep"]
async fn issue_311_tx_index_rekey_repeated_cycles() -> Result<()> {
    let mut failures = Vec::new();
    for idx in [Idx::None, Idx::BTree, Idx::Hash] {
        failures.extend(cycles_once(idx).await?);
    }
    assert!(failures.is_empty(), "failing cycles: {failures:#?}");
    Ok(())
}

/// Flushes of an idle L0 must not throttle a transaction's writes.
///
/// While a transaction holds its writes in its private buffer, the shared L0
/// is empty, so a flush landing then writes nothing. Each one still counted as
/// a flush generation (`l1_runs`). Back-to-back flushes — the stress test's
/// flusher, or the auto-flush timer — pushed the count past the write
/// throttle's hard limit (32), and the transaction's first `SET` write stalled
/// until compaction folded the runs: measured at ~9.8 s for a SET that takes
/// ~0.26 s with no flusher (1428 empty flushes, 0 non-empty, during one SET;
/// no prefetch errors, no per-row storage reads).
///
/// Asserted on the counters, not on time: the flushes are issued before the
/// SET, so without the fix `l1_runs` climbs past the hard limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn issue_311_idle_flushes_do_not_throttle_a_transaction() -> Result<()> {
    // Compaction off, so nothing lowers `l1_runs` behind the test's back; with
    // it off, the throttle refuses a write at the hard limit (`WriteStalled`)
    // instead of waiting, so the unfixed SET fails rather than stalls.
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        strict_schema: true,
        ..Default::default()
    };
    config.compaction.enabled = false;
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("Concept")
        .property("sid", DataType::Int)
        .property("view", DataType::String)
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute_with("UNWIND range(0, $n - 1) AS i CREATE (:Concept {sid: i, view: 'old'})")
        .param("n", Value::Int(500))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let runs_before = db.metrics().l1_run_count;
    assert!(runs_before >= 1, "premise: the load flushed a run");

    let tx = s.tx().await?;
    // More idle flushes than the default hard limit.
    let hard_limit = UniConfig::default().throttle.hard_limit;
    for _ in 0..hard_limit + 8 {
        db.flush().await?;
    }
    assert_eq!(
        db.metrics().l1_run_count,
        runs_before,
        "flushes of an empty L0 counted as flush generations"
    );
    let waits_before = db.metrics().write_throttle_waits;
    let set = tx
        .query("MATCH (n:Concept) WHERE n.view = 'old' SET n.view = 'new' RETURN id(n) AS vid")
        .await?;
    assert_eq!(set.rows().len(), 500);
    assert_eq!(
        db.metrics().write_throttle_waits,
        waits_before,
        "the transaction's SET waited on the write throttle"
    );
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        db.metrics().l1_run_count,
        runs_before + 1,
        "the flush of the committed SET is a generation"
    );
    Ok(())
}
