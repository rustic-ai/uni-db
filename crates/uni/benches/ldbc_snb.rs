// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! LDBC SNB Interactive — load SF1 and report ingest cost per phase.
//!
//! Stage 1 of E3. The gating unknown for the whole item is whether SF1 (~746 MB
//! across 31 CSVs) ingests in tractable time: the largest graph benchmark in this
//! repo before now was 1000 nodes, so there is no precedent to extrapolate from.
//! This measures it before anything is built on top, the same way `ann_pareto`
//! measured its corpus build before trusting a recall curve.
//!
//! # Running
//!
//! ```bash
//! python3 scripts/fixtures/fetch.py --only ldbc-sf1-person   # ... and the rest
//! TMPDIR=$HOME/uni-bench-tmp cargo bench -p uni-db --bench ldbc_snb
//! ```
//!
//! `TMPDIR` must be on real disk. `Uni::temporary()` lands in
//! `std::env::temp_dir()`, and a tmpfs `/tmp` will run out of space part way
//! through a load of this size — which now surfaces as a flush-barrier error
//! rather than a silently half-written dataset, but still fails the run.

#![recursion_limit = "256"]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use uni_db::{DataType, Uni, UniConfig};

#[path = "ldbc/mod.rs"]
mod ldbc;
#[path = "ldbc/params.rs"]
mod params;

use ldbc::{
    FileStat, IdMap, edge_type_from_filename, fixture, load_edges, load_nodes, parse_header,
};

/// Node files, in load order. Every node file must land before any edge file:
/// edges are resolved through the id -> VID maps these produce.
const NODE_FILES: &[(&str, &str)] = &[
    ("Person", "ldbc-sf1-person"),
    ("Forum", "ldbc-sf1-forum"),
    ("Post", "ldbc-sf1-post"),
    ("Comment", "ldbc-sf1-comment"),
    ("Tag", "ldbc-sf1-tag"),
    ("TagClass", "ldbc-sf1-tagclass"),
    ("Place", "ldbc-sf1-place"),
    ("Organisation", "ldbc-sf1-organisation"),
];

/// Edge files as `(fixture, src entity, dst entity)`. The edge *type* is derived
/// from the file name so the loader and LDBC's query text agree by construction.
const EDGE_FILES: &[(&str, &str, &str, &str)] = &[
    (
        "Person_knows_Person",
        "ldbc-sf1-person-knows-person",
        "Person",
        "Person",
    ),
    (
        "Person_hasInterest_Tag",
        "ldbc-sf1-person-hasinterest-tag",
        "Person",
        "Tag",
    ),
    (
        "Person_isLocatedIn_Place",
        "ldbc-sf1-person-islocatedin-place",
        "Person",
        "Place",
    ),
    (
        "Person_likes_Post",
        "ldbc-sf1-person-likes-post",
        "Person",
        "Post",
    ),
    (
        "Person_likes_Comment",
        "ldbc-sf1-person-likes-comment",
        "Person",
        "Comment",
    ),
    (
        "Person_studyAt_Organisation",
        "ldbc-sf1-person-studyat-organisation",
        "Person",
        "Organisation",
    ),
    (
        "Person_workAt_Organisation",
        "ldbc-sf1-person-workat-organisation",
        "Person",
        "Organisation",
    ),
    (
        "Post_hasCreator_Person",
        "ldbc-sf1-post-hascreator-person",
        "Post",
        "Person",
    ),
    ("Post_hasTag_Tag", "ldbc-sf1-post-hastag-tag", "Post", "Tag"),
    (
        "Post_isLocatedIn_Place",
        "ldbc-sf1-post-islocatedin-place",
        "Post",
        "Place",
    ),
    (
        "Comment_hasCreator_Person",
        "ldbc-sf1-comment-hascreator-person",
        "Comment",
        "Person",
    ),
    (
        "Comment_hasTag_Tag",
        "ldbc-sf1-comment-hastag-tag",
        "Comment",
        "Tag",
    ),
    (
        "Comment_isLocatedIn_Place",
        "ldbc-sf1-comment-islocatedin-place",
        "Comment",
        "Place",
    ),
    (
        "Comment_replyOf_Post",
        "ldbc-sf1-comment-replyof-post",
        "Comment",
        "Post",
    ),
    (
        "Comment_replyOf_Comment",
        "ldbc-sf1-comment-replyof-comment",
        "Comment",
        "Comment",
    ),
    (
        "Forum_containerOf_Post",
        "ldbc-sf1-forum-containerof-post",
        "Forum",
        "Post",
    ),
    (
        "Forum_hasMember_Person",
        "ldbc-sf1-forum-hasmember-person",
        "Forum",
        "Person",
    ),
    (
        "Forum_hasModerator_Person",
        "ldbc-sf1-forum-hasmoderator-person",
        "Forum",
        "Person",
    ),
    (
        "Forum_hasTag_Tag",
        "ldbc-sf1-forum-hastag-tag",
        "Forum",
        "Tag",
    ),
    (
        "Organisation_isLocatedIn_Place",
        "ldbc-sf1-organisation-islocatedin-place",
        "Organisation",
        "Place",
    ),
    (
        "Place_isPartOf_Place",
        "ldbc-sf1-place-ispartof-place",
        "Place",
        "Place",
    ),
    (
        "Tag_hasType_TagClass",
        "ldbc-sf1-tag-hastype-tagclass",
        "Tag",
        "TagClass",
    ),
    (
        "TagClass_isSubclassOf_TagClass",
        "ldbc-sf1-tagclass-issubclassof-tagclass",
        "TagClass",
        "TagClass",
    ),
];

/// Concrete labels a `:LABEL` column splits an entity into. LDBC's Cypher matches
/// on these, not on the supertype.
const SPLIT_LABELS: &[(&str, &[&str])] = &[
    // The supertype is listed too: LDBC rows carry `Place;City`, and the loader
    // now stores both, so both must be declared.
    ("Place", &["Place", "City", "Country", "Continent"]),
    ("Organisation", &["Organisation", "University", "Company"]),
];

/// Concrete labels an entity is stored under, expanding any `:LABEL` split.
fn labels_of(entity: &str) -> Vec<&'static str> {
    SPLIT_LABELS.iter().find(|(e, _)| *e == entity).map_or_else(
        || vec![Box::leak(entity.to_string().into_boxed_str()) as &'static str],
        |(_, ls)| ls.to_vec(),
    )
}

/// Declare a label from its file's header, so the schema follows the data.
async fn declare_from_header(db: &Uni, label: &str, header_line: &str) -> anyhow::Result<()> {
    let roles = parse_header(header_line);
    let mut b = db.schema().label(label).property("id", DataType::Int64);
    for role in &roles {
        if let ldbc::ColRole::Prop(name, ty) = role {
            b = b.property(name, ty.to_uni());
        }
    }
    b.done().apply().await?;
    Ok(())
}

fn first_line(path: &std::path::Path) -> String {
    use std::io::{BufRead, BufReader};
    let f = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    BufReader::new(f)
        .lines()
        .next()
        .and_then(Result::ok)
        .unwrap_or_default()
}

/// The 14 Interactive complex reads, vendored **byte-for-byte** from
/// `ldbc/ldbc_snb_interactive_v1_impls` (Apache-2.0).
///
/// They are not edited. uni-db targets openCypher and passes the TCK, and these
/// queries are plain openCypher, so any edit that turned out to be necessary
/// would be a finding about a semantic gap — not routine adaptation — and would
/// be recorded here with its reason. So far the list is empty.
const QUERIES: &[(&str, &str)] = &[
    ("IC1", include_str!("ldbc/queries/ic1.cypher")),
    ("IC2", include_str!("ldbc/queries/ic2.cypher")),
    ("IC3", include_str!("ldbc/queries/ic3.cypher")),
    ("IC4", include_str!("ldbc/queries/ic4.cypher")),
    ("IC5", include_str!("ldbc/queries/ic5.cypher")),
    ("IC6", include_str!("ldbc/queries/ic6.cypher")),
    ("IC7", include_str!("ldbc/queries/ic7.cypher")),
    ("IC8", include_str!("ldbc/queries/ic8.cypher")),
    ("IC9", include_str!("ldbc/queries/ic9.cypher")),
    ("IC10", include_str!("ldbc/queries/ic10.cypher")),
    ("IC11", include_str!("ldbc/queries/ic11.cypher")),
    ("IC12", include_str!("ldbc/queries/ic12.cypher")),
    ("IC13", include_str!("ldbc/queries/ic13.cypher")),
    ("IC14", include_str!("ldbc/queries/ic14.cypher")),
];

/// Outcome of one complex read.
struct QueryRun {
    name: &'static str,
    rows: usize,
    ms: f64,
    error: Option<String>,
}

/// Run one query with only the parameters it actually references — the set
/// differs per query and binding unused names is rejected.
async fn run_query(
    db: &Uni,
    cypher: &str,
    all: &HashMap<String, uni_db::Value>,
) -> anyhow::Result<(usize, f64, Vec<Vec<String>>)> {
    // The session must outlive the builder that borrows it.
    let session = db.session();
    let mut q = session.query_with(cypher);
    for (k, v) in all {
        if cypher.contains(&format!("${k}")) {
            q = q.param(k, v.clone());
        }
    }
    let t = Instant::now();
    let rows = q.fetch_all().await?;
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    // Stringified so the oracle can emit the same shape without both sides
    // sharing a serialization library.
    let dump: Vec<Vec<String>> = rows
        .iter()
        .map(|r| r.values().iter().map(|v| format!("{v:?}")).collect())
        .collect();
    Ok((rows.len(), ms, dump))
}

fn main() {
    if std::env::args().any(|a| a == "--test") {
        eprintln!("[ldbc] --test mode: this bench loads SF1; skipping");
        return;
    }

    let rt = Runtime::new().unwrap();
    let config = UniConfig {
        // The 30s default is an interactive-latency guard, not a bulk-load
        // budget; `ann_pareto` hit the same wall on a 1M-row build.
        query_timeout: Duration::from_secs(1800),
        // `commit_timeout` defaults to 5s, and a bulk commit outgrows it as the
        // dataset accumulates: the first edge files committed fine and HAS_TAG
        // (~700k rows, arriving after ~3.4M rows were already resident) did not.
        // Interactive default, bulk workload — same mismatch as above.
        commit_timeout: Duration::from_secs(1800),
        ..Default::default()
    };

    let total = Instant::now();
    // `LDBC_DB=<path>` keeps the loaded graph on disk and reuses it when it is
    // already populated. A full SF1 load is ~4 minutes, which is far too slow an
    // iteration loop for working on the queries or the parameter derivation.
    let persist = std::env::var("LDBC_DB").ok();
    let db = match &persist {
        Some(path) => rt
            .block_on(Uni::open(path).config(config).build())
            .expect("open persistent db"),
        None => rt
            .block_on(Uni::temporary().config(config).build())
            .expect("open db"),
    };

    let already_loaded = persist.is_some()
        && rt
            .block_on(async {
                db.session()
                    .query("MATCH (p:Person) RETURN count(p) AS c")
                    .await
                    .ok()
                    .and_then(|r| r.rows().first().and_then(|row| row.get::<i64>("c").ok()))
            })
            .unwrap_or(0)
            > 0;
    if already_loaded {
        eprintln!(
            "[ldbc] reusing the graph already in {}",
            persist.as_deref().unwrap_or("")
        );
    }

    // --- schema, declared from the CSV headers -----------------------------
    if !already_loaded {
        eprintln!("[ldbc] declaring schema…");
        for (entity, fixture_name) in NODE_FILES {
            let path = fixture(fixture_name);
            let header = first_line(&path);
            let labels: Vec<&str> = SPLIT_LABELS
                .iter()
                .find(|(e, _)| e == entity)
                .map_or_else(|| vec![*entity], |(_, ls)| ls.to_vec());
            for label in labels {
                rt.block_on(declare_from_header(&db, label, &header))
                    .unwrap_or_else(|e| panic!("declare {label}: {e}"));
            }
        }
        // Several LDBC edge types span more than one endpoint pair — IS_LOCATED_IN
        // connects Person, Post, Comment and Organisation to places; HAS_TAG and
        // LIKES and REPLY_OF likewise. So the src/dst label sets are aggregated per
        // type and the type is declared once, rather than per file.
        let mut edge_labels: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
        for (stem, _, src, dst) in EDGE_FILES {
            let et = edge_type_from_filename(stem).expect("edge type from filename");
            let e = edge_labels.entry(et).or_default();
            for l in labels_of(src) {
                if !e.0.iter().any(|x| x == l) {
                    e.0.push(l.to_string());
                }
            }
            for l in labels_of(dst) {
                if !e.1.iter().any(|x| x == l) {
                    e.1.push(l.to_string());
                }
            }
        }
        for (et, (from, to)) in &edge_labels {
            let from_r: Vec<&str> = from.iter().map(String::as_str).collect();
            let to_r: Vec<&str> = to.iter().map(String::as_str).collect();
            rt.block_on(db.schema().edge_type(et, &from_r, &to_r).apply())
                .unwrap_or_else(|e| panic!("declare edge {et}: {e}"));
        }

        // --- nodes -------------------------------------------------------------
        let mut ids: IdMap = HashMap::new();
        let mut node_stats: Vec<FileStat> = Vec::new();
        for (entity, fixture_name) in NODE_FILES {
            let path = fixture(fixture_name);
            let stat = rt
                .block_on(load_nodes(&db, entity, &path, &mut ids))
                .unwrap_or_else(|e| panic!("load nodes {entity}: {e}"));
            eprintln!(
                "[ldbc]   nodes {:<14} {:>9} rows  {:>7.1}s",
                stat.name, stat.rows, stat.secs
            );
            node_stats.push(stat);
        }

        // --- edges -------------------------------------------------------------
        let mut edge_stats: Vec<FileStat> = Vec::new();
        let mut total_unresolved = 0usize;
        for (stem, fixture_name, src, dst) in EDGE_FILES {
            let et = edge_type_from_filename(stem).unwrap();
            let path = fixture(fixture_name);
            let (stat, unresolved) = rt
                .block_on(load_edges(&db, &et, src, dst, &path, &ids))
                .unwrap_or_else(|e| panic!("load edges {et}: {e}"));
            if unresolved > 0 {
                eprintln!("[ldbc]   WARN {et}: {unresolved} rows had an unresolved endpoint");
            }
            total_unresolved += unresolved;
            eprintln!(
                "[ldbc]   edges {:<22} {:>9} rows  {:>7.1}s",
                stat.name, stat.rows, stat.secs
            );
            edge_stats.push(stat);
        }

        // --- flush -------------------------------------------------------------
        let t = Instant::now();
        rt.block_on(db.flush()).expect("flush");
        let flush = t.elapsed().as_secs_f64();

        report(
            &node_stats,
            &edge_stats,
            total_unresolved,
            flush,
            total.elapsed().as_secs_f64(),
        );
    }

    eprintln!("[ldbc] deriving substitution parameters…");
    let bound = rt
        .block_on(params::derive(&db))
        .expect("derive substitution parameters");
    let mut names: Vec<&String> = bound.keys().collect();
    names.sort();
    println!("\n## Parameters\n");
    for k in names {
        println!("- `{k}` = `{:?}`", bound[k]);
    }

    let out_dir = std::env::var("LDBC_OUT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("ldbc-results"));
    std::fs::create_dir_all(&out_dir).expect("create result dir");

    let mut runs: Vec<QueryRun> = Vec::new();
    for (name, cypher) in QUERIES {
        match rt.block_on(run_query(&db, cypher, &bound)) {
            Ok((rows, ms, dump)) => {
                std::fs::write(out_dir.join(format!("{name}.tsv")), format_rows(&dump))
                    .expect("write result");
                eprintln!("[ldbc]   {name:<5} {rows:>6} rows  {ms:>8.1} ms");
                runs.push(QueryRun {
                    name,
                    rows,
                    ms,
                    error: None,
                });
            }
            Err(e) => {
                eprintln!("[ldbc]   {name:<5} ERROR {e}");
                runs.push(QueryRun {
                    name,
                    rows: 0,
                    ms: 0.0,
                    error: Some(e.to_string()),
                });
            }
        }
    }
    query_report(&runs, &out_dir);
}

/// Newline-delimited rows, each tab-joined. Deliberately plain so the oracle can
/// emit the same shape without a shared serialization library.
fn format_rows(rows: &[Vec<String>]) -> String {
    rows.iter()
        .map(|r| r.join("\t"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn query_report(runs: &[QueryRun], out_dir: &std::path::Path) {
    println!("\n## Interactive complex reads\n");
    println!("| query | rows | ms |");
    println!("|---|---:|---:|");
    for r in runs {
        match &r.error {
            Some(e) => println!("| {} | — | **error**: {} |", r.name, e.replace('|', "-")),
            None => println!("| {} | {} | {:.1} |", r.name, r.rows, r.ms),
        }
    }
    println!("\nResults written to `{}`\n", out_dir.display());

    let failed: Vec<&QueryRun> = runs.iter().filter(|r| r.error.is_some()).collect();
    if !failed.is_empty() {
        eprintln!(
            "[ldbc] {} of {} queries failed to execute: {}",
            failed.len(),
            runs.len(),
            failed.iter().map(|r| r.name).collect::<Vec<_>>().join(", ")
        );
        std::process::exit(1);
    }

    // The differential oracle compares two engines, and two engines agreeing on
    // an EMPTY result agree about nothing. An empty result is therefore a failure
    // of the parameter set, not a passing query — this is the check that keeps
    // the comparison meaningful.
    let empty: Vec<&str> = runs
        .iter()
        .filter(|r| r.rows == 0)
        .map(|r| r.name)
        .collect();
    if !empty.is_empty() {
        eprintln!(
            "[ldbc] VACUOUS: {} returned no rows. A query that selects nothing agrees with any \
             oracle, so these parameters cannot validate anything. Widen them in \
             benches/ldbc/params.rs.",
            empty.join(", ")
        );
        std::process::exit(1);
    }
    println!("[ldbc] all {} queries returned rows", runs.len());
}

fn report(nodes: &[FileStat], edges: &[FileStat], unresolved: usize, flush: f64, wall: f64) {
    let n_rows: usize = nodes.iter().map(|s| s.rows).sum();
    let e_rows: usize = edges.iter().map(|s| s.rows).sum();

    println!("\n## LDBC SNB SF1 ingest\n");
    println!("| phase | rows | seconds |");
    println!("|---|---:|---:|");
    println!(
        "| nodes | {n_rows} | {:.1} |",
        nodes.iter().map(|s| s.secs).sum::<f64>()
    );
    println!(
        "| edges | {e_rows} | {:.1} |",
        edges.iter().map(|s| s.secs).sum::<f64>()
    );
    println!("| flush | — | {flush:.1} |");
    println!("| **total** | **{}** | **{wall:.1}** |", n_rows + e_rows);
    println!();

    // --- non-vacuity -------------------------------------------------------
    //
    // A load that silently produced nothing would make every downstream query
    // agree with an equally empty oracle, so the load asserts its own shape
    // before any query is run.
    if n_rows == 0 || e_rows == 0 {
        eprintln!("[ldbc] VACUOUS: loaded {n_rows} nodes and {e_rows} edges");
        std::process::exit(1);
    }
    if unresolved > 0 {
        eprintln!(
            "[ldbc] {unresolved} edge rows had an unresolved endpoint. Every LDBC edge must \
             reference a node present in the same scale factor, so this means the id -> VID map \
             is incomplete — not a data quirk."
        );
        std::process::exit(1);
    }
    println!("[ldbc] loaded {n_rows} nodes and {e_rows} edges, no unresolved endpoints");
}
