// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A disk-backed database that survives close-and-reopen, for crash/recovery
//! tests.
//!
//! The harness owns a `TempDir` and opens a database rooted inside it. Because
//! the directory outlives any individual `Uni` handle, a test can:
//!
//! ```ignore
//! let h = DiskHarness::new()?;
//! {
//!     let db = h.open().await?;          // first boot
//!     // ... commit some transactions ...
//!     db.flush().await?;                 // durable
//! }                                      // db dropped == process "stops"
//! let db = h.open().await?;              // reboot: WAL replays from disk
//! // ... assert recovered state ...
//! ```
//!
//! Reopening the same path replays the WAL, exercising the real recovery path —
//! unlike in-memory tests, this validates the durability boundary end-to-end.

use anyhow::Result;
use tempfile::TempDir;
use uni_db::{Uni, UniConfig};

/// Owns a temp directory and opens databases rooted at `<dir>/db`.
pub struct DiskHarness {
    dir: TempDir,
}

impl DiskHarness {
    /// Creates a fresh temp directory. The directory (and all data) is removed
    /// when the harness is dropped.
    pub fn new() -> Result<Self> {
        Ok(Self {
            dir: tempfile::tempdir()?,
        })
    }

    /// The database root URI (a local filesystem path under the temp dir).
    pub fn uri(&self) -> String {
        self.dir.path().join("db").to_string_lossy().into_owned()
    }

    /// Opens (or, on first call, creates) the database at this harness's path.
    /// Call repeatedly to simulate reboots; each reopen replays the WAL.
    pub async fn open(&self) -> Result<Uni> {
        Ok(Uni::open(self.uri()).build().await?)
    }

    /// Like [`open`](Self::open) but with an explicit [`UniConfig`].
    ///
    /// Used by recovery tests that must pin flush behavior (e.g. keep a commit
    /// in the WAL rather than letting a background auto-flush promote it to L1).
    pub async fn open_with(&self, config: UniConfig) -> Result<Uni> {
        Ok(Uni::open(self.uri()).config(config).build().await?)
    }

    /// Simulates a power loss that tore the last commit's WAL segment:
    /// overwrites the newest segment holding mutations with garbage.
    ///
    /// Tests stand a dropped handle in for the crash, and a closing database
    /// seals its WAL with an empty segment recording which commits were
    /// announced (`Writer::seal_wal`). A power loss during the last commit's
    /// write never gets that far, so those trailing empty segments are
    /// removed first.
    ///
    /// # Panics
    ///
    /// When no segment holds a mutation.
    pub fn tear_wal_tail(&self) -> Result<()> {
        let wal_dir = std::path::PathBuf::from(self.uri()).join("wal");
        let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(&wal_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "wal"))
            .collect();
        segments.sort();
        while let Some(last) = segments.last() {
            let segment = uni_store::runtime::wal::decode_segment(&std::fs::read(last)?)
                .map_err(|e| anyhow::anyhow!("WAL segment {}: {e}", last.display()))?;
            if !segment.mutations.is_empty() {
                break;
            }
            std::fs::remove_file(last)?;
            segments.pop();
        }
        let tail = segments
            .last()
            .expect("at least one WAL segment holding a commit");
        std::fs::write(tail, b"torn-by-power-loss")?;
        Ok(())
    }
}
