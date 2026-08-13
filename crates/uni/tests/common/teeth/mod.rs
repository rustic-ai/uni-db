// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Teeth — evidence that the suite catches the bugs that motivated it.
//!
//! # What a "tooth" is
//!
//! Not a test. A **revert validation**: a checked-in patch that reinstates a
//! historical defect, plus a record of what failed when it was applied. A test
//! that has only ever been seen passing is not evidence it can catch anything —
//! it may assert something the bug never violated, or route around the fix site
//! entirely.
//!
//! The suite already contains an example of each failure mode:
//!
//! * **Non-discriminating.** Of the three tests in
//!   `bugs/hash_index_range_quoting.rs`, `range_still_constrains_when_it_
//!   excludes_the_equality` asserts the result is empty — and the range-fusion
//!   bug *also* produced empty. It passes identically with and without the
//!   defect. It is a sound test of the opposite direction and a non-witness for
//!   the bug beside it.
//! * **Routes around the fix site.** `build_edge_adjacency_and_target_props`
//!   (#135) has exactly one caller, under `GraphTraverseMainStream`, which is
//!   planned only when *every* requested relationship type is absent from the
//!   schema (`planner.rs:5405`). The #135 regression test declares its label but
//!   never its `PARENT` edge type — that omission is load-bearing, and a fixture
//!   that declares its edge types cannot reach the bug however wide the
//!   generator gets.
//!
//! # Why there are no new `#[tokio::test]`s here
//!
//! Five of the six bugs already have regression suites (twelve tests for #97
//! alone). Writing a seventh near-copy would add maintenance surface and detect
//! nothing new. What was missing was never another assertion — it was evidence
//! that the existing ones bite. So the deliverable is
//! `scripts/testing/teeth_validate.sh`, the patches under
//! `docs/testing/reverts/`, and the ledger in
//! `docs/testing/teeth-2026-08-13.md`.
//!
//! The one test that does live here guards the patches themselves.

use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    // `CARGO_MANIFEST_DIR` is `crates/uni`.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("repo root is two levels above crates/uni")
        .to_path_buf()
}

/// Every revert patch must still apply to the current tree.
///
/// A tooth's validation is only meaningful against a patch that still describes
/// today's fix. When a fix site moves, this fails and forces the patch to be
/// regenerated — which is the moment to re-validate the tooth, rather than
/// discovering six months later that the evidence has been stale since.
///
/// Deliberately `git apply --check` and not the full harness: this is
/// milliseconds and needs no build, so it belongs in the PR lane.
/// `teeth_validate.sh` builds the workspace once per bug and is run by hand or
/// nightly.
///
/// This checks only that the patch *applies*, never that the defect it
/// reinstates is still catchable — that claim needs the harness, and conflating
/// the two would let a rotted tooth report itself healthy.
#[test]
fn every_revert_patch_still_applies() {
    let root = repo_root();
    let dir = root.join("docs/testing/reverts");
    let mut patches: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "patch"))
        .collect();
    patches.sort();

    assert!(
        !patches.is_empty(),
        "no revert patches in {} — the teeth ledger has lost its evidence",
        dir.display()
    );

    let mut stale = Vec::new();
    for patch in &patches {
        let out = Command::new("git")
            .arg("-C")
            .arg(&root)
            .arg("apply")
            .arg("--check")
            .arg(patch)
            .output()
            .expect("git apply --check could not be run");
        if !out.status.success() {
            stale.push(format!(
                "  {}: {}",
                patch.file_name().unwrap_or_default().to_string_lossy(),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
    }

    assert!(
        stale.is_empty(),
        "revert patches no longer apply — the fix site moved, so the tooth is \
         unvalidated until the patch is regenerated and re-run through \
         scripts/testing/teeth_validate.sh:\n{}",
        stale.join("\n")
    );
}
