// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! LDBC SNB Interactive loader: CSV -> uni-db.
//!
//! The fixture is LDBC datagen SF1 output renamed to **Neo4j admin-import**
//! convention: pipe-delimited, typed headers (`id:ID(Person)`,
//! `:START_ID(Person)`, `creationDate:LONG`), and dates as epoch milliseconds.
//! The schema is read from those headers rather than hardcoded, so a scale
//! factor with different columns loads without code changes.
//!
//! # Why a Rust loader rather than `COPY ... FROM`
//!
//! Cypher's `COPY` edge branch parses its src/dst columns as **raw VIDs**
//! (`uni-query/src/query/executor/read.rs`), and LDBC references nodes by
//! external id. Nothing in the stack resolves an external id to a VID, so the
//! loader builds that map itself: `bulk_insert_vertices` returns the allocated
//! VIDs *in input order*, which is the only handle on the mapping. Every node
//! file must therefore load before any edge file.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use uni_db::{DataType, Uni, Value, Vid};

// --------------------------------------------------------------------------
// fixture resolution
// --------------------------------------------------------------------------

fn fetch_py() -> PathBuf {
    // Cargo runs benches with CWD at the *package* root, not the workspace root.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/fixtures/fetch.py")
}

/// Resolve one pinned fixture to a local path, verifying its digest first.
///
/// Panics with the exact fetch command rather than degrading to a smaller or
/// synthetic corpus — a benchmark quietly running on the wrong data is the
/// failure this suite exists to catch.
pub fn fixture(name: &str) -> PathBuf {
    let out = Command::new("python3")
        .arg(fetch_py())
        .args(["--print-path", "--only", name])
        .output()
        .expect("run scripts/fixtures/fetch.py");
    assert!(
        out.status.success(),
        "fixture {name} unavailable:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    PathBuf::from(String::from_utf8_lossy(&out.stdout).trim())
}

// --------------------------------------------------------------------------
// header parsing
// --------------------------------------------------------------------------

/// The column types LDBC's converted CSVs actually use.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LdbcType {
    Long,
    Int,
    Str,
    /// `STRING[]` — semicolon-separated inside the field.
    StrArray,
    /// `DATE_EPOCHMS` — milliseconds since the epoch, same wire form as `LONG`.
    DateEpochMs,
}

impl LdbcType {
    fn parse(spec: &str) -> Option<Self> {
        match spec {
            "LONG" => Some(Self::Long),
            "INT" => Some(Self::Int),
            "STRING" => Some(Self::Str),
            "STRING[]" => Some(Self::StrArray),
            "DATE_EPOCHMS" => Some(Self::DateEpochMs),
            _ => None,
        }
    }

    pub fn to_uni(self) -> DataType {
        match self {
            // Epoch millis stay integers: LDBC's Cypher compares `creationDate`
            // against millisecond literals, so converting to a temporal type
            // here would silently change what those comparisons mean.
            Self::Long | Self::Int | Self::DateEpochMs => DataType::Int64,
            Self::Str => DataType::String,
            Self::StrArray => DataType::List(Box::new(DataType::String)),
        }
    }

    fn value(self, raw: &str) -> Value {
        match self {
            Self::Long | Self::Int | Self::DateEpochMs => {
                Value::Int(raw.parse::<i64>().unwrap_or_default())
            }
            Self::Str => Value::String(raw.to_string()),
            Self::StrArray => Value::List(
                raw.split(';')
                    .filter(|s| !s.is_empty())
                    .map(|s| Value::String(s.to_string()))
                    .collect(),
            ),
        }
    }
}

/// What a column means in the Neo4j-import header convention.
#[derive(Debug, Clone)]
pub enum ColRole {
    /// `id:ID(Person)` — the node's external id.
    Id,
    /// `:START_ID(X)` / `:END_ID(X)` — edge endpoints, by external id.
    StartId(String),
    EndId(String),
    /// `:LABEL` — Neo4j multi-label discriminator. Place carries
    /// City/Country/Continent here and Organisation carries University/Company.
    Label,
    Prop(String, LdbcType),
    /// A column shape this loader does not model; skipped rather than guessed.
    Ignored,
}

/// Parse one pipe-delimited header line into column roles.
pub fn parse_header(line: &str) -> Vec<ColRole> {
    line.trim_end_matches(['\r', '\n'])
        .split('|')
        .map(|col| {
            if col.starts_with(":START_ID(") {
                ColRole::StartId(entity_of(col))
            } else if col.starts_with(":END_ID(") {
                ColRole::EndId(entity_of(col))
            } else if col == ":LABEL" {
                ColRole::Label
            } else if col.starts_with("id:ID(") {
                ColRole::Id
            } else if let Some((name, ty)) = col.split_once(':') {
                LdbcType::parse(ty).map_or(ColRole::Ignored, |t| ColRole::Prop(name.to_string(), t))
            } else {
                ColRole::Ignored
            }
        })
        .collect()
}

/// `:START_ID(Person)` -> `Person`.
fn entity_of(col: &str) -> String {
    col.split_once('(')
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .unwrap_or_default()
        .to_string()
}

/// `Comment_hasCreator_Person.csv` -> `HAS_CREATOR`.
///
/// LDBC's Cypher names relationships in UPPER_SNAKE, so the edge type is derived
/// from the file name rather than configured, keeping the loader and the query
/// text consistent by construction.
pub fn edge_type_from_filename(stem: &str) -> Option<String> {
    let parts: Vec<&str> = stem.split('_').collect();
    if parts.len() < 3 {
        return None;
    }
    let camel = parts[1];
    let mut out = String::new();
    for (i, ch) in camel.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(ch.to_ascii_uppercase());
    }
    Some(out)
}

// --------------------------------------------------------------------------
// loading
// --------------------------------------------------------------------------

/// Rows per `bulk_insert_*` call. Matches the batch size `ann_pareto` settled on.
const BATCH: usize = 10_000;

/// External id -> VID, per entity type.
///
/// A plain map rather than a densified vector: LDBC ids are sparse across
/// entities, and at SF1 this is ~3M entries, which is affordable.
pub type IdMap = HashMap<String, HashMap<i64, Vid>>;

/// Rows staged for one label set before insert: external ids alongside their
/// property maps, kept positionally aligned.
type StagedRows = (Vec<i64>, Vec<HashMap<String, Value>>);

/// Timings for one loaded file, so ingest cost is reported per phase rather than
/// as a single opaque total.
pub struct FileStat {
    pub name: String,
    pub rows: usize,
    pub secs: f64,
}

fn read_lines(path: &Path) -> (Vec<ColRole>, Vec<String>) {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut it = text.lines();
    let header = parse_header(it.next().unwrap_or_default());
    (header, it.map(str::to_string).collect())
}

/// Load one node file, returning its per-label id -> VID maps.
///
/// `:LABEL`-bearing files (Place, Organisation) are split by that column so each
/// row lands under its concrete label — `City`, `Country`, `University` — which
/// is what LDBC's Cypher matches on. `bulk_insert_vertices` takes a single
/// label, so the split happens here.
pub async fn load_nodes(
    db: &Uni,
    entity: &str,
    path: &Path,
    ids: &mut IdMap,
) -> anyhow::Result<FileStat> {
    let (header, lines) = read_lines(path);
    let t = Instant::now();
    let mut rows = 0usize;

    // label-set -> (external ids, property maps). Keyed by the *set* so a
    // `Place;City` row and a `Place;Country` row are inserted under their own
    // label sets rather than being flattened together.
    let mut staged: HashMap<Vec<String>, StagedRows> = HashMap::new();

    for line in &lines {
        let cols: Vec<&str> = line.split('|').collect();
        let mut ext_id = 0i64;
        let mut labels: Vec<String> = vec![entity.to_string()];
        let mut props: HashMap<String, Value> = HashMap::new();
        for (i, role) in header.iter().enumerate() {
            let Some(raw) = cols.get(i) else { continue };
            match role {
                ColRole::Id => {
                    ext_id = raw.parse().unwrap_or_default();
                    props.insert("id".to_string(), Value::Int(ext_id));
                }
                ColRole::Label => {
                    // Neo4j's `:LABEL` column carries *all* of a node's labels,
                    // semicolon-separated — Place rows read `Place;City`. Keep
                    // every one of them: LDBC's Cypher matches on the subtype
                    // (`:City`, `:University`) while the supertype (`:Place`)
                    // is equally part of the data, and dropping either would
                    // make a legitimate query silently return nothing.
                    labels = raw
                        .split(';')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .collect();
                }
                ColRole::Prop(name, ty) => {
                    props.insert(name.clone(), ty.value(raw));
                }
                _ => {}
            }
        }
        labels.sort();
        let e = staged.entry(labels).or_default();
        e.0.push(ext_id);
        e.1.push(props);
        rows += 1;
    }

    let tx = db.session().tx().await?;
    for (label_set, (ext_ids, props)) in staged {
        let refs: Vec<&str> = label_set.iter().map(String::as_str).collect();
        for (id_chunk, prop_chunk) in ext_ids.chunks(BATCH).zip(props.chunks(BATCH)) {
            let vids = tx
                .bulk_insert_vertices_labeled(&refs, prop_chunk.to_vec())
                .await?;
            assert_eq!(
                vids.len(),
                id_chunk.len(),
                "bulk_insert_vertices returned {} VIDs for {} rows — the id->VID \
                 mapping relies on a positional correspondence",
                vids.len(),
                id_chunk.len()
            );
            // Index the id map under every label the row carries, so an edge
            // file declaring `:END_ID(Place)` resolves against rows stored as
            // `Place;City` without needing to know the subtype.
            for l in &label_set {
                let map = ids.entry(l.clone()).or_default();
                for (ext, vid) in id_chunk.iter().zip(&vids) {
                    map.insert(*ext, *vid);
                }
            }
            let map = ids.entry(entity.to_string()).or_default();
            for (ext, vid) in id_chunk.iter().zip(vids) {
                map.insert(*ext, vid);
            }
        }
    }
    tx.commit().await?;

    Ok(FileStat {
        name: entity.to_string(),
        rows,
        secs: t.elapsed().as_secs_f64(),
    })
}

/// Look up a VID by external id, trying every label an entity may have been
/// split into (`Place` -> City/Country/Continent).
fn resolve(ids: &IdMap, entity: &str, ext: i64) -> Option<Vid> {
    if let Some(m) = ids.get(entity)
        && let Some(v) = m.get(&ext)
    {
        return Some(*v);
    }
    ids.values().find_map(|m| m.get(&ext).copied())
}

/// Load one edge file. Returns the stat plus how many rows were dropped because
/// an endpoint was missing — reported, never silently ignored.
pub async fn load_edges(
    db: &Uni,
    edge_type: &str,
    src_entity: &str,
    dst_entity: &str,
    path: &Path,
    ids: &IdMap,
) -> anyhow::Result<(FileStat, usize)> {
    let (header, lines) = read_lines(path);
    let t = Instant::now();
    let mut edges: Vec<(Vid, Vid, HashMap<String, Value>)> = Vec::with_capacity(lines.len());
    let mut unresolved = 0usize;

    for line in &lines {
        let cols: Vec<&str> = line.split('|').collect();
        let (mut src, mut dst) = (None, None);
        let mut props: HashMap<String, Value> = HashMap::new();
        for (i, role) in header.iter().enumerate() {
            let Some(raw) = cols.get(i) else { continue };
            match role {
                ColRole::StartId(_) => src = raw.parse::<i64>().ok(),
                ColRole::EndId(_) => dst = raw.parse::<i64>().ok(),
                ColRole::Prop(name, ty) => {
                    props.insert(name.clone(), ty.value(raw));
                }
                _ => {}
            }
        }
        match (
            src.and_then(|s| resolve(ids, src_entity, s)),
            dst.and_then(|d| resolve(ids, dst_entity, d)),
        ) {
            (Some(s), Some(d)) => edges.push((s, d, props)),
            _ => unresolved += 1,
        }
    }

    let rows = edges.len();
    let tx = db.session().tx().await?;
    for chunk in edges.chunks(BATCH) {
        tx.bulk_insert_edges(edge_type, chunk.to_vec()).await?;
    }
    tx.commit().await?;

    Ok((
        FileStat {
            name: edge_type.to_string(),
            rows,
            secs: t.elapsed().as_secs_f64(),
        },
        unresolved,
    ))
}
