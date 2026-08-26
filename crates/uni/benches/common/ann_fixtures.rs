// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fixture resolution and `.fvecs` / `.ivecs` readers for the ANN benchmarks.
//!
//! Pulled into a bench with the repo's shared-module idiom:
//!
//! ```ignore
//! #[path = "common/ann_fixtures.rs"]
//! mod ann_fixtures;
//! ```
//!
//! # Why resolution shells out
//!
//! `scripts/fixtures/fetch.py` is the single implementation of the digest logic
//! (see `docs/fixtures.md`). Resolving through it means there is no Rust twin to
//! drift from it and no `sha2` dependency here. `--print-path` verifies the file
//! against its pinned digest *before* printing, so a path that comes back is a
//! path that matched.
//!
//! # Why a missing fixture is fatal
//!
//! It never degrades to a smaller corpus or a synthetic substitute. Silently
//! substituting generated data is how `fork_index_recall_bench.rs` came to report
//! recall@10 = 1.000 while the index under test never ran.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

/// Absolute path to `scripts/fixtures/fetch.py`.
///
/// Cargo runs benches with CWD at the *package* root (`crates/uni`), not the
/// workspace root, so a CWD-relative path would miss.
fn fetch_py() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/fixtures/fetch.py")
}

/// Resolve a pinned fixture to a local path, fetching nothing.
///
/// # Panics
/// Panics with the exact fetch command when the fixture is absent or fails
/// verification.
pub fn fixture(name: &str) -> PathBuf {
    let out = Command::new("python3")
        .arg(fetch_py())
        .args(["--print-path", "--only", name])
        .output()
        .expect("run scripts/fixtures/fetch.py");
    assert!(
        out.status.success(),
        "fixture {name} unavailable:\n{}\n  run: python3 scripts/fixtures/fetch.py --only {name}",
        String::from_utf8_lossy(&out.stderr),
    );
    PathBuf::from(String::from_utf8_lossy(&out.stdout).trim())
}

/// Structural header of a `.?vecs` file: a repeated `[i32 dim][dim * 4 bytes]`.
///
/// Returns `(dim, count)`. The count is *derived* from the file length and the
/// dimension read out of the first record, then checked to divide exactly.
///
/// This is the probe that a digest cannot perform. A digest proves the bytes are
/// the ones that were pinned; it says nothing about whether the *right* fixture
/// was pinned into a given slot. `sift_query.fvecs` and `sift_base.fvecs` are
/// both well-formed 128-dimensional fvecs files, so only the arithmetic
/// distinguishes 10 000 vectors from 1 000 000.
fn vecs_shape(path: &Path, expect_dim: usize) -> (usize, usize) {
    let len = std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()))
        .len() as usize;
    let mut header = [0u8; 4];
    {
        use std::io::Read;
        let mut fh =
            std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
        fh.read_exact(&mut header)
            .unwrap_or_else(|e| panic!("read header of {}: {e}", path.display()));
    }
    let dim = i32::from_le_bytes(header) as usize;
    assert_eq!(
        dim,
        expect_dim,
        "{}: first record declares dim {dim}, expected {expect_dim}",
        path.display()
    );
    let record = 4 + dim * 4;
    assert_eq!(
        len % record,
        0,
        "{}: length {len} is not a whole number of {record}-byte records — truncated or not a \
         .?vecs file",
        path.display()
    );
    (dim, len / record)
}

/// Read a `.fvecs` file: `n` records of `[i32 dim][dim * f32]`, little-endian.
///
/// `limit` caps how many vectors are returned; the whole file is still validated
/// structurally first, so a truncated fixture fails even when only a prefix is
/// wanted.
pub fn read_fvecs(path: &Path, expect_dim: usize, limit: Option<usize>) -> Vec<Vec<f32>> {
    let (dim, count) = vecs_shape(path, expect_dim);
    let take = limit.unwrap_or(count).min(count);
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let record = 4 + dim * 4;
    let mut out = Vec::with_capacity(take);
    for i in 0..take {
        let base = i * record;
        // Every record repeats the dimension. A file that changes dimension
        // partway is malformed, and silently reading past it would corrupt the
        // corpus in a way no digest would catch.
        let d = i32::from_le_bytes(bytes[base..base + 4].try_into().unwrap()) as usize;
        assert_eq!(d, dim, "{}: record {i} declares dim {d}", path.display());
        let mut v = Vec::with_capacity(dim);
        for j in 0..dim {
            let o = base + 4 + j * 4;
            v.push(f32::from_le_bytes(bytes[o..o + 4].try_into().unwrap()));
        }
        out.push(v);
    }
    out
}

/// Read an `.ivecs` file: `n` records of `[i32 d][d * i32]`, little-endian.
///
/// SIFT's `sift_groundtruth.ivecs` holds the true top-100 base indices per query
/// under **L2**, which is what makes recall here externally defined rather than
/// scored against an oracle of our own.
pub fn read_ivecs(path: &Path, expect_dim: usize, limit: Option<usize>) -> Vec<Vec<u32>> {
    let (dim, count) = vecs_shape(path, expect_dim);
    let take = limit.unwrap_or(count).min(count);
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let record = 4 + dim * 4;
    let mut out = Vec::with_capacity(take);
    for i in 0..take {
        let base = i * record;
        let d = i32::from_le_bytes(bytes[base..base + 4].try_into().unwrap()) as usize;
        assert_eq!(d, dim, "{}: record {i} declares dim {d}", path.display());
        let mut v = Vec::with_capacity(dim);
        for j in 0..dim {
            let o = base + 4 + j * 4;
            v.push(i32::from_le_bytes(bytes[o..o + 4].try_into().unwrap()) as u32);
        }
        out.push(v);
    }
    out
}

/// Squared L2 distance in f64 — the metric SIFT's ground truth is defined under.
///
/// Squared rather than rooted because only the ordering is used, and the square
/// root is a monotone transform that costs a million calls per query.
pub fn l2_sq(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(&x, &y)| {
            let d = x as f64 - y as f64;
            d * d
        })
        .sum()
}
