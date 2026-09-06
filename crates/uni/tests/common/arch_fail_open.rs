// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A ratchet on the three fail-open decisions #233 settled project-wide.
//!
//! #233's 27 catalogued sites were fixed in `uni-store` and `uni-query`, and
//! the two project-wide decisions behind them — *a failed read never returns a
//! default*, and *a failed index-status write is never unrecorded* — were
//! applied within the crates that were audited. A later audit of the crates
//! the issue scoped out found both mechanisms fully intact there: the same
//! "index status Online lie" in `uni-bulk`, and not-found classified by
//! matching the error's rendered text in two more places.
//!
//! **The audit boundary, not the mechanism, is what bounded the last fix.**
//! That is what this test exists to stop. It is deliberately NOT a general
//! "no swallowed error" scan: `unwrap_or_default` is ordinary Rust and most
//! uses are correct, so a broad rule would carry a budget of hundreds and the
//! real entries would drown — the failure mode this project has already
//! watched sink one class review. It enforces three narrow rules instead,
//! each with a canonical remedy and each with a real instance found in a
//! crate the previous pass did not reach.
//!
//! If you are here because this test failed, the rule that fired names the
//! remedy. Each budget is a ratchet: it may go down, never up.

use std::path::{Path, PathBuf};

/// Rule A — an index dataset opened with `.ok()` cannot tell "never built"
/// from "broken", so a search silently returns zero hits. Discriminate with
/// `store_utils::is_dataset_not_found`, as `storage/index.rs` does.
const RULE_A: (&str, &str) = (
    "Dataset::open",
    "an index dataset opened with `.ok()` reads a broken index as an unbuilt one; \
     discriminate with store_utils::is_dataset_not_found",
);

/// Rule B — a dropped index-status write leaves the index reporting its
/// previous status, so a gate or the planner trusts a stale `Online`.
const RULE_B: (&str, &str) = (
    "update_index_metadata",
    "an index-status write must never be dropped with `let _ =`; record it \
     (propagate where there is a caller, else log and count)",
);

/// Rule C — classifying not-found by matching the error's rendered text
/// swallows any genuine failure whose message happens to contain the phrase.
const RULE_C: (&str, &str) = (
    "contains(\"not found\")",
    "classify not-found with store_utils::is_not_found / is_dataset_not_found, \
     or ask the schema, rather than matching error text",
);

/// Files permitted to break a rule, with the count and the reason. Audited;
/// none is a leftover. Lower a number only by removing a use.
fn budget(rule: &str) -> Vec<(&'static str, usize)> {
    match rule {
        // `store_utils` IS the typed classifier, and `resilient_store` documents
        // in a comment why the string form was wrong. Both are tests//docs of
        // the rule rather than violations of it.
        "contains(\"not found\")" => vec![
            ("crates/uni-store/src/store_utils.rs", 1),
            ("crates/uni-store/src/storage/resilient_store.rs", 2),
        ],
        _ => Vec::new(),
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root is two levels above crates/uni")
        .to_path_buf()
}

fn source_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            source_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// True when `line` both mentions `needle` and swallows its result.
///
/// Line-scoped on purpose: `let _ = foo(...)` and `foo(...).ok()` are the two
/// shapes that discard a `Result`, and both sit on the statement's own line in
/// rustfmt'd code.
fn swallows(line: &str, needle: &str) -> bool {
    if !line.contains(needle) {
        return false;
    }
    let trimmed = line.trim_start();
    trimmed.starts_with("let _ =") || line.contains(").ok()") || line.contains("await).ok()")
}

#[test]
fn no_new_site_reopens_a_settled_fail_open_decision() {
    let root = workspace_root();
    let crates = root.join("crates");
    let mut files = Vec::new();
    source_files(&crates, &mut files);
    assert!(
        files.len() > 100,
        "found only {} source files under {} — the walk is not finding the tree, so a green \
         result here would prove nothing",
        files.len(),
        crates.display()
    );

    let mut problems = Vec::new();

    for (needle, remedy) in [RULE_A, RULE_B, RULE_C] {
        let allowed = budget(needle);
        let mut actual: Vec<(String, usize)> = Vec::new();

        for file in &files {
            let rel = file
                .strip_prefix(&root)
                .unwrap_or(file)
                .to_string_lossy()
                .replace('\\', "/");
            // `src` only: a test may construct a swallowed error on purpose.
            if !rel.contains("/src/") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(file) else {
                continue;
            };
            let hits = if needle == RULE_C.0 {
                // Rule C is about the classification itself, not about
                // discarding a Result, so it is a plain substring rule.
                text.matches(needle).count()
            } else {
                text.lines().filter(|l| swallows(l, needle)).count()
            };
            if hits > 0 {
                actual.push((rel, hits));
            }
        }

        for (file, count) in &actual {
            let permitted = allowed
                .iter()
                .find(|(f, _)| *f == file.as_str())
                .map_or(0, |(_, n)| *n);
            if *count > permitted {
                problems.push(format!(
                    "{file}: {count} site(s) matching `{needle}`, budget {permitted}.\n    {remedy}"
                ));
            }
        }
        for (file, permitted) in &allowed {
            let count = actual
                .iter()
                .find(|(f, _)| f == file)
                .map_or(0, |(_, n)| *n);
            if count < *permitted {
                problems.push(format!(
                    "{file}: budget {permitted} for `{needle}` but only {count} found — lower \
                     it to {count} (or remove the entry) so the ratchet stays tight."
                ));
            }
        }
    }

    assert!(problems.is_empty(), "\n{}", problems.join("\n"));
}
