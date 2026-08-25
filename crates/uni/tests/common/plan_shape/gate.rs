// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The operator-activation gate.
//!
//! # What it prevents
//!
//! On 2026-08-14 a coverage run found `df_graph/vid_lookup_join.rs` at **0 of
//! 441 executable lines** — while the 15-test suite written for that operator
//! was green. The tests asserted result bags; the operator is contractually
//! bag-identical to the `HashJoinExec` it replaces; so no test could tell
//! "fired" from "silently fell back". Six guards had quietly narrowed until the
//! documented use case could not reach it at all.
//!
//! Coverage alone is not the answer either, and the same survey showed why:
//! `PowerStepExec` and `GraphGatherStepExec` (1,044 lines) are constructed
//! **only** from their own `#[cfg(test)] mod tests`, so they report *nonzero*
//! coverage while no query can ever produce them. That is the same blind spot
//! from the other side.
//!
//! So this gate does not measure coverage. It requires every physical operator
//! to be **classified**, and classified with evidence.
//!
//! # Cost
//!
//! Pure string scanning over ~60 source files and the test tree. No database,
//! no async, no build beyond the binary that already exists — the same argument
//! as `teeth::every_revert_patch_still_applies`, which belongs in the PR lane
//! for the same reason.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use super::registry::{MAX_UNPROVEN, OPERATORS, Status};

/// Directories scanned for `impl ExecutionPlan for`.
///
/// Two roots, not one: `StorageScanExec` lives in `uni-plugin-builtin`, outside
/// `df_graph/`. A single-root scan would silently exempt it.
const SCAN_ROOTS: &[&str] = &[
    "crates/uni-query/src/query/df_graph",
    "crates/uni-plugin-builtin/src",
];

/// Directories scanned for proof-of-emission assertions.
const TEST_ROOTS: &[&str] = &["crates/uni/tests", "crates/uni-query/tests"];

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `crates/uni`.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repo root is two levels above crates/uni")
        .to_path_buf()
}

fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(rust_files(&p));
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
    out
}

/// Every type with an `impl ExecutionPlan for`, read from source.
///
/// Line-prefix matching rather than a parser: all 31 impls are written on one
/// line, and a formatting variant that defeats this shows up as a *missing*
/// impl — i.e. it fails the gate rather than silently exempting something.
fn impls_in_source(root: &Path) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for dir in SCAN_ROOTS {
        for file in rust_files(&root.join(dir)) {
            let Ok(text) = std::fs::read_to_string(&file) else {
                continue;
            };
            for line in text.lines() {
                if let Some(rest) = line.trim().strip_prefix("impl ExecutionPlan for ") {
                    let ty: String = rest
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect();
                    if !ty.is_empty() {
                        found.insert(ty);
                    }
                }
            }
        }
    }
    found
}

/// Concatenation of every test source file, for proof lookups.
fn all_test_sources(root: &Path) -> String {
    let mut buf = String::new();
    for dir in TEST_ROOTS {
        for file in rust_files(&root.join(dir)) {
            if let Ok(t) = std::fs::read_to_string(&file) {
                buf.push_str(&t);
                buf.push('\n');
            }
        }
    }
    buf
}

/// Does `haystack` contain an assertion helper call naming `op`?
///
/// Requiring the operator to appear **as an argument to the helper** is the
/// whole point. A bare substring search would be satisfied by a doc comment,
/// and this repo comments heavily — every operator name already appears in
/// prose somewhere. Only the helper actually profiles a query and compares
/// against the executed plan, so only a helper call is evidence.
fn has_proof_call(haystack: &str, op: &str) -> bool {
    let needle = format!("\"{op}\"");
    haystack.lines().any(|l| {
        (l.contains("assert_plan_uses(")
            || l.contains("assert_plan_uses_any(")
            || l.contains("assert_uses(")
            || l.contains("assert_uses_any("))
            && l.contains(&needle)
    }) || {
        // Multi-line call: helper on one line, operator literal on a later one.
        let mut open = false;
        for l in haystack.lines() {
            if l.contains("assert_plan_uses(")
                || l.contains("assert_plan_uses_any(")
                || l.contains("assert_uses(")
                || l.contains("assert_uses_any(")
            {
                open = true;
            }
            if open && l.contains(&needle) {
                return true;
            }
            if open && l.contains(';') {
                open = false;
            }
        }
        false
    }
}

/// **The gate.** Every `ExecutionPlan` impl must be classified, and every
/// classification must be backed by something checkable.
#[test]
fn every_execution_operator_is_classified() {
    let root = repo_root();
    let in_source = impls_in_source(&root);
    let in_registry: BTreeSet<String> = OPERATORS.iter().map(|o| o.ty.to_string()).collect();

    assert!(
        !in_source.is_empty(),
        "the source scan found no `impl ExecutionPlan for` at all — the scan \
         roots are wrong, and this gate is silently vouching for nothing"
    );

    let missing: Vec<&String> = in_source.difference(&in_registry).collect();
    assert!(
        missing.is_empty(),
        "{} ExecutionPlan impl(s) have no entry in plan_shape/registry.rs:\n{}\n\n\
         An unclassified operator can stop being emitted with nothing turning \
         red: a bag-comparing test passes identically whether an optimization \
         fired or fell back, and an operator built only from its own \
         `#[cfg(test)]` still shows nonzero coverage. That is how \
         vid_lookup_join.rs reached 0/441 executed lines under a dedicated \
         15-test suite.\n\n\
         Add a row with ONE of:\n  \
         Status::Proven {{ by, in_file }} — a test calling \
         assert_plan_uses(.., \"<name>\")\n  \
         Status::Unreachable {{ reason }} — no planner path can emit it\n\n\
         `Unproven` exists for the operators that predate this gate, but the \
         count is ratcheted (see MAX_UNPROVEN), so a NEW operator cannot use \
         it — it must arrive proven or explicitly unreachable.",
        missing.len(),
        missing
            .iter()
            .map(|m| format!("  {m}"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    let stale: Vec<&String> = in_registry.difference(&in_source).collect();
    assert!(
        stale.is_empty(),
        "registry.rs has row(s) whose `impl ExecutionPlan for` no longer exists:\n{}\n\n\
         A stale row keeps vouching for an operator that is gone, and hides the \
         next real one behind a passing gate.",
        stale
            .iter()
            .map(|m| format!("  {m}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// A `Proven` claim must be backed by an assertion that would actually go red.
#[test]
fn proven_operators_have_a_real_assertion() {
    let root = repo_root();
    let tests = all_test_sources(&root);

    for op in OPERATORS {
        let Status::Proven { by, in_file } = op.status else {
            continue;
        };
        let path = root.join(in_file);
        let src = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "registry names {in_file} as the proof for {}, but it cannot be read: {e}",
                op.ty
            )
        });
        assert!(
            src.contains(&format!("fn {by}")),
            "registry claims {} is Proven by `{by}` in {in_file}, but that file \
             defines no such test.",
            op.ty
        );
        assert!(
            has_proof_call(&tests, op.runtime_name),
            "registry claims {} is Proven by `{by}`, but no test anywhere calls \
             an assertion helper with the literal \"{}\".\n\n\
             A Proven claim means: some test would go red if this operator \
             stopped being emitted. Naming it in a comment does not do that — \
             which is exactly how a 15-test suite coexisted with 0 executed \
             lines. Add\n    \
             plan_shape::assert_plan_uses(&session, query, \"{}\").await;\n\
             to `{by}`, or point `by` at the test that already has it.",
            op.ty,
            op.runtime_name,
            op.runtime_name
        );
    }
}

/// An `Unreachable` claim must still be true, and must say why.
#[test]
fn unreachable_operators_stay_unreachable() {
    let root = repo_root();
    let tests = all_test_sources(&root);

    for op in OPERATORS {
        let Status::Unreachable { reason } = op.status else {
            continue;
        };
        assert!(
            reason.len() >= 40 && !reason.contains("TODO") && !reason.contains("FIXME"),
            "{}: an Unreachable reason is the artifact a future reader decides \
             wire-it-up-or-delete-it from. Give the actual planner reason, not \
             a placeholder. Got: {reason:?}",
            op.ty
        );
        assert!(
            !has_proof_call(&tests, op.runtime_name),
            "{} is marked Unreachable, but a test asserts it is emitted.\n\n\
             One of the two is stale. If the planner now emits it, flip the row \
             to Proven in this same change — an Unreachable reason that has \
             quietly become false is worse than no row at all.",
            op.ty
        );
    }
}

/// **The ratchet.** The number of unproven operators may never grow.
///
/// `Unproven` is an honest state — most of these operators plainly *are*
/// reachable, they simply have no assertion — but it must not become a resting
/// place. Bounding the count means a new operator has to arrive `Proven` or
/// `Unreachable`, and every retrofit tightens the bound permanently.
#[test]
fn unproven_operator_count_only_ratchets_down() {
    let unproven: Vec<&str> = OPERATORS
        .iter()
        .filter(|o| matches!(o.status, Status::Unproven))
        .map(|o| o.runtime_name)
        .collect();

    assert!(
        unproven.len() <= MAX_UNPROVEN,
        "{} operators are Unproven, over the ratchet of {MAX_UNPROVEN}:\n{}\n\n\
         A new operator must arrive Proven (a test calls assert_plan_uses for \
         it) or Unreachable (no planner path can emit it). Raising \
         MAX_UNPROVEN defeats the point — it is the number that makes this gap \
         countable.",
        unproven.len(),
        unproven
            .iter()
            .map(|u| format!("  {u}"))
            .collect::<Vec<_>>()
            .join("\n")
    );

    assert_eq!(
        unproven.len(),
        MAX_UNPROVEN,
        "MAX_UNPROVEN is {MAX_UNPROVEN} but only {} rows are Unproven. Someone \
         retrofitted a proof without lowering the bound — do that in the same \
         change, or the ratchet silently loosens by exactly the amount just won.",
        unproven.len()
    );
}

/// Every `name()` string literal must be some row's `runtime_name`.
///
/// This is the only thing standing between a rename and every plan assertion in
/// the suite going quietly false: `assert_plan_uses(.., "GraphScanExec")`
/// against an operator that now reports `"GraphScan"` fails loudly, but the
/// reverse — a *negative* assertion — would start passing for free.
#[test]
fn runtime_names_match_the_operator_impls() {
    let root = repo_root();
    let known: BTreeSet<&str> = OPERATORS.iter().map(|o| o.runtime_name).collect();

    let mut unknown: Vec<String> = Vec::new();
    for dir in SCAN_ROOTS {
        for file in rust_files(&root.join(dir)) {
            let Ok(text) = std::fs::read_to_string(&file) else {
                continue;
            };
            let lines: Vec<&str> = text.lines().collect();
            // Only `name()` **inside an `impl ExecutionPlan` block** counts.
            // `uni-plugin-builtin` is full of unrelated `fn name()` methods —
            // collations, logical types, plugin metadata — and an unscoped
            // harvest reported all of them as unregistered operators. Track the
            // enclosing impl instead: rustfmt closes a top-level block with `}`
            // in column 0, which is a reliable enough terminator here.
            let mut in_exec_impl = false;
            for (i, line) in lines.iter().enumerate() {
                if line.starts_with("impl ExecutionPlan for ") {
                    in_exec_impl = true;
                } else if in_exec_impl && *line == "}" {
                    in_exec_impl = false;
                }
                if !in_exec_impl || !line.trim().starts_with("fn name(&self)") {
                    continue;
                }
                // The literal is on this line or the next few.
                for probe in lines.iter().skip(i).take(4) {
                    if let Some(start) = probe.find('"') {
                        let rest = &probe[start + 1..];
                        if let Some(end) = rest.find('"') {
                            let lit = &rest[..end];
                            if !lit.is_empty() && !known.contains(lit) {
                                unknown.push(format!("{lit} (in {})", file.display()));
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    assert!(
        unknown.is_empty(),
        "operator name() literals not present as any registry `runtime_name`:\n{}\n\n\
         Assertions match on this string. A rename that the registry does not \
         follow turns every negative assertion into a free pass.",
        unknown.join("\n")
    );
}
