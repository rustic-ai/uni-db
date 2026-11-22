// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for `crates/uni-cli/src/demo/semantic_scholar.rs:88`.
//!
//! The import calls `w.insert_vertex(vid, uni_props, None)`, and
//! `Writer::insert_vertex` forwards a hard-coded empty label slice. So papers
//! are inserted with an EMPTY label set even though the demo schema registers a
//! `Paper` vertex type. A label-scoped query `MATCH (p:Paper) ...` therefore
//! finds nothing, while a label-agnostic `MATCH (p) ...` finds the vertices —
//! proving the vertices exist but carry no `Paper` label.
//!
//! This test drives the REAL compiled `uni` binary end to end: `uni import`
//! then two `uni query` invocations, parsing the `count(*)` cell out of the
//! rendered table.

use std::path::{Path, PathBuf};
use std::process::Command;

/// A unique temp directory for this test's throwaway artifacts.
fn unique_tmp_dir(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!(
        "uni_cli_repro_{tag}_{}_{nanos}",
        std::process::id()
    ));
    p
}

/// Parse the single integer value out of a rendered prettytable, i.e. the lone
/// `| <n> |` data line produced by a one-column, one-row `count(*)` result.
fn parse_single_count(stdout: &str) -> Option<u64> {
    for line in stdout.lines() {
        let t = line.trim();
        if let Some(inner) = t.strip_prefix('|').and_then(|s| s.strip_suffix('|'))
            && let Ok(n) = inner.trim().parse::<u64>()
        {
            return Some(n);
        }
    }
    None
}

/// Run `uni query --path <db> <statement>` and return its stdout.
fn run_query(bin: &str, db_path: &Path, statement: &str) -> String {
    let output = Command::new(bin)
        .arg("query")
        .arg("--path")
        .arg(db_path)
        .arg(statement)
        .output()
        .expect("failed to spawn uni binary");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Imported papers land label-less: `MATCH (p:Paper)` counts 0, `MATCH (p)`
/// counts all of them.
#[test]
fn imported_papers_have_no_paper_label() {
    let bin = env!("CARGO_BIN_EXE_uni");
    let work = unique_tmp_dir("label");
    std::fs::create_dir_all(&work).unwrap();

    // Three real papers + one citation, in the JSONL shape the importer reads.
    let papers = work.join("papers.jsonl");
    std::fs::write(
        &papers,
        "{\"vid\":1,\"title\":\"Attention Is All You Need\",\"year\":2017,\"citation_count\":100000}\n\
         {\"vid\":2,\"title\":\"BERT\",\"year\":2018,\"citation_count\":80000}\n\
         {\"vid\":3,\"title\":\"GPT-3\",\"year\":2020,\"citation_count\":50000}\n",
    )
    .unwrap();

    let citations = work.join("citations.jsonl");
    std::fs::write(&citations, "{\"src_vid\":2,\"dst_vid\":1}\n").unwrap();

    let db_path = work.join("db");

    let import = Command::new(bin)
        .arg("import")
        .arg("semantic-scholar")
        .arg("--papers")
        .arg(&papers)
        .arg("--citations")
        .arg(&citations)
        .arg("--output")
        .arg(&db_path)
        .output()
        .expect("failed to spawn uni import");
    assert!(
        import.status.success(),
        "import failed: stdout=\n{}\nstderr=\n{}",
        String::from_utf8_lossy(&import.stdout),
        String::from_utf8_lossy(&import.stderr)
    );

    let label_scoped = run_query(bin, &db_path, "MATCH (p:Paper) RETURN count(p)");
    let label_agnostic = run_query(bin, &db_path, "MATCH (p) RETURN count(p)");

    let paper_count = parse_single_count(&label_scoped);
    let all_count = parse_single_count(&label_agnostic);

    eprintln!("--- MATCH (p:Paper) stdout:\n{label_scoped}");
    eprintln!("--- MATCH (p)       stdout:\n{label_agnostic}");
    eprintln!("--- parsed: paper={paper_count:?}, all={all_count:?}");

    let _ = std::fs::remove_dir_all(&work);

    // The vertices exist: a label-agnostic scan finds all 3.
    assert_eq!(
        all_count,
        Some(3),
        "expected 3 vertices via label-agnostic scan; parsed {all_count:?}"
    );

    // FIXED (semantic_scholar.rs): the importer now attaches the `Paper` label,
    // so a label-scoped scan finds all 3 papers.
    assert_eq!(
        paper_count,
        Some(3),
        "expected 3 papers for MATCH (p:Paper); parsed {paper_count:?}"
    );
}

/// `uni import <name>` runs the importer the name selects; an unknown name
/// fails, lists the available importers, and writes nothing.
#[test]
fn import_rejects_unknown_dataset_name() {
    let bin = env!("CARGO_BIN_EXE_uni");
    let work = unique_tmp_dir("unknown_import");
    std::fs::create_dir_all(&work).unwrap();

    let papers = work.join("papers.jsonl");
    std::fs::write(&papers, "{\"vid\":1,\"title\":\"A\",\"year\":2020}\n").unwrap();
    let citations = work.join("citations.jsonl");
    std::fs::write(&citations, "").unwrap();
    let db_path = work.join("db");

    let import = Command::new(bin)
        .arg("import")
        .arg("support-bot")
        .arg("--papers")
        .arg(&papers)
        .arg("--citations")
        .arg(&citations)
        .arg("--output")
        .arg(&db_path)
        .output()
        .expect("failed to spawn uni import");
    let stdout = String::from_utf8_lossy(&import.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&import.stderr).into_owned();
    let db_created = db_path.exists();
    let _ = std::fs::remove_dir_all(&work);

    assert!(
        !import.status.success(),
        "import of an unknown dataset succeeded: stdout=\n{stdout}\nstderr=\n{stderr}"
    );
    assert!(
        stderr.contains("support-bot") && stderr.contains("semantic-scholar"),
        "error should name the dataset and the available importers; stderr=\n{stderr}"
    );
    assert!(
        !db_created,
        "an unknown dataset must not create the output DB"
    );
}
