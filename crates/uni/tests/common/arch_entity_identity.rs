// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A ratchet on hand-rolled entity identity and shape (#234).
//!
//! Entity identity has one accessor — `Value::entity_ref` / `entity_vid` /
//! `entity_eid` / `entity_ref_from_map`. Reading `_vid`, `_eid` or `_id` out of
//! a map by hand re-derives it, and every such site has silently answered "not
//! an entity" for the encoding it did not match, which callers read as "not
//! equal", "not a duplicate" or "no such row".
//!
//! Comparison, dedup and hashing are handled at the boundary — `Value`'s own
//! `PartialEq`/`Hash` compare entities by identity — so a site cannot get those
//! wrong any more. Extraction is what remains, and Rust gives no cheap way to
//! forbid a string literal at compile time. This test is that guard instead:
//! CI-time rather than compile-time, and a **ratchet**, not a ban. The counts
//! below are what exists today; a new hand-rolled site fails this test, and
//! every count is free to go down.
//!
//! If you are here because this test failed: use the accessor. If you are here
//! because you *removed* a site, lower its number (or delete the entry).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Files still permitted to read an entity id out of a map by hand, how many
/// times each does, and **why that use is not identity extraction**. Every entry
/// here was audited; none is a leftover. Lower a number only by removing a use.
///
/// `crates/uni-common/src/value.rs` is absent deliberately: it *is* the
/// accessor, and is excluded by the walk below rather than budgeted here.
fn budget() -> BTreeMap<&'static str, usize> {
    BTreeMap::from([
        // `"_vid"` here is a *result-column alias* this code emits itself —
        // `RETURN id(n) AS _vid, n` — read back off a Locy `FactRow`. The value
        // is a scalar from `id(n)`, and the row is not an entity map at all.
        // Routing would tie an internal alias to the entity-encoding contract.
        ("crates/uni/src/api/impl_locy.rs", 3),
        // `_eid`-presence guards on the SET / REMOVE / DELETE arms, plus the
        // node-vs-edge map discriminator. These decide *whether to mutate*, and
        // the accessor is deliberately wider: it also answers "edge" for a map
        // carrying only endpoints or a type name. Widening a delete guard
        // changes what gets deleted.
        ("crates/uni-query/src/query/executor/write.rs", 3),
        // `is_node_map` / `is_edge_map` dispatch discriminators. Their leniency
        // is deliberate and paired with `normalize_property_value`, so that user
        // data merely *containing* `_vid` is not converted into a node.
        (
            "crates/uni-query/src/query/executor/result_normalizer.rs",
            2,
        ),
        // A presence-and-nullness probe for an all-null OPTIONAL MATCH struct,
        // not an id read. `entity_ref` returns `None` both for "key absent" and
        // "key present but null", collapsing the distinction this depends on —
        // `properties(n)` on an unmatched OPTIONAL MATCH would return `{}`
        // instead of null.
        ("crates/uni-query-functions/src/df_udfs.rs", 2),
    ])
}

/// Files still permitted to read an entity's labels, edge type or endpoints out
/// of a map by hand. Same rule as [`budget`]: audited, and the ratchet only
/// turns down.
///
/// Every entry here is a *decoder* or a *dispatch guard* — code whose job is to
/// recognise the map form and convert it, or to decide which of two shapes it
/// has. Those must read the keys; that is what makes them the boundary. A
/// reader that merely wants the labels of an entity it already has does not
/// belong here.
fn shape_budget() -> BTreeMap<&'static str, usize> {
    BTreeMap::from([
        // `is_node_map` / `is_edge_map` and the map -> native conversion they
        // guard. Reading the keys *is* the conversion.
        (
            "crates/uni-query/src/query/executor/result_normalizer.rs",
            4,
        ),
        // `extract_edge_identity` and the SET/REMOVE edge-map dispatch guard:
        // they decide whether a value is an edge *map* specifically, and the
        // accessor is deliberately wider. Plus `resolve_edge_type_id`, which
        // takes the raw value so it can accept either an id or a name.
        ("crates/uni-query/src/query/executor/write.rs", 10),
        // Locy's row decoder converting its own map vocabulary to native.
        ("crates/uni-query/src/query/df_graph/locy_eval.rs", 3),
        // `edge_eid_and_type`'s vertex twin, decoding a map to native.
        ("crates/uni-query/src/query/df_graph/mutation_common.rs", 1),
        // Sort-key payload encoders that are map-shaped by construction; the
        // native forms have their own encoders alongside.
        ("crates/uni-query-functions/src/df_udfs/sort_key.rs", 3),
        // `labels()`'s map arm, which must accept a `_labels`-only map that
        // carries no id at all — wider than `entity_labels` allows.
        ("crates/uni-query-functions/src/df_udfs.rs", 1),
    ])
}

/// The extraction idiom: pulling an entity id straight out of a map.
const PATTERNS: [&str; 3] = ["get(\"_vid\")", "get(\"_eid\")", "get(\"_id\")"];

/// The same idiom for an entity's *shape* rather than its id: labels, edge
/// type, endpoints. Each of these had the identical failure — several
/// spellings, a different subset understood at each site, and a silent wrong
/// answer for whichever the reader did not know. They now have accessors too:
/// `Value::entity_labels`, `Value::edge_type_ref`, `Value::edge_endpoints`.
const SHAPE_PATTERNS: [&str; 8] = [
    "get(\"_labels\")",
    "get(\"_type\")",
    "get(\"_type_name\")",
    "get(\"edge_type\")",
    "get(\"_src\")",
    "get(\"_dst\")",
    "get(\"_src_vid\")",
    "get(\"_dst_vid\")",
];

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<root>/crates/uni`.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root is two levels above crates/uni")
        .to_path_buf()
}

/// Recursively collect `.rs` files under `crates/*/src`.
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

#[test]
fn no_new_site_hand_rolls_entity_identity() {
    let root = workspace_root();
    let crates = root.join("crates");
    let mut files = Vec::new();
    source_files(&crates, &mut files);
    assert!(
        files.len() > 100,
        "found only {} source files under {} — the walk is not finding the tree, \
         so a green result here would prove nothing",
        files.len(),
        crates.display()
    );

    let budget = budget();
    let mut actual: BTreeMap<String, usize> = BTreeMap::new();
    let mut shape_actual: BTreeMap<String, usize> = BTreeMap::new();

    for file in &files {
        let rel = file
            .strip_prefix(&root)
            .unwrap_or(file)
            .to_string_lossy()
            .replace('\\', "/");
        // Only `src`, not tests: a test may construct either encoding on
        // purpose, which is the point of several of them.
        if !rel.contains("/src/") {
            continue;
        }
        // The accessor itself.
        if rel == "crates/uni-common/src/value.rs" {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(file) else {
            continue;
        };
        let hits: usize = PATTERNS.iter().map(|p| text.matches(p).count()).sum();
        if hits > 0 {
            actual.insert(rel.clone(), hits);
        }
        let shape_hits: usize = SHAPE_PATTERNS.iter().map(|p| text.matches(p).count()).sum();
        if shape_hits > 0 {
            shape_actual.insert(rel, shape_hits);
        }
    }

    let mut problems = Vec::new();
    for (file, count) in &actual {
        match budget.get(file.as_str()) {
            None => problems.push(format!(
                "{file}: {count} hand-rolled entity-id read(s) in a file with no budget. \
                 Use Value::entity_ref / entity_vid / entity_eid / entity_ref_from_map."
            )),
            Some(&allowed) if *count > allowed => problems.push(format!(
                "{file}: {count} hand-rolled entity-id read(s), budget {allowed}. \
                 The ratchet only turns one way — route the new one through the accessor."
            )),
            Some(_) => {}
        }
    }

    // A budget entry that is now too generous should be tightened, so the
    // ratchet keeps its teeth as sites are fixed.
    for (file, allowed) in &budget {
        let count = actual.get(*file).copied().unwrap_or(0);
        if count < *allowed {
            problems.push(format!(
                "{file}: budget {allowed} but only {count} found — lower it to {count} \
                 (or remove the entry) so the ratchet stays tight."
            ));
        }
    }

    // The same ratchet for labels / edge type / endpoints.
    let shape = shape_budget();
    for (file, count) in &shape_actual {
        match shape.get(file.as_str()) {
            None => problems.push(format!(
                "{file}: {count} hand-rolled read(s) of an entity's labels, edge type or \
                 endpoints in a file with no budget. Use Value::entity_labels / \
                 edge_type_ref / edge_endpoints."
            )),
            Some(&allowed) if *count > allowed => problems.push(format!(
                "{file}: {count} hand-rolled shape read(s), budget {allowed}. \
                 The ratchet only turns one way — route the new one through the accessor."
            )),
            Some(_) => {}
        }
    }
    for (file, allowed) in &shape {
        let count = shape_actual.get(*file).copied().unwrap_or(0);
        if count < *allowed {
            problems.push(format!(
                "{file}: shape budget {allowed} but only {count} found — lower it to {count} \
                 (or remove the entry) so the ratchet stays tight."
            ));
        }
    }

    assert!(problems.is_empty(), "\n{}", problems.join("\n"));
}
