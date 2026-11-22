// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Multi-agent access — read-only mode and write lease abstraction.
//!
//! Supports multi-reader / single-writer architectures where multiple
//! processes can open the same database for reads, while writes are
//! coordinated through a lease mechanism.

use uni_common::Result;

/// Write lease strategy: how a writable handle keeps a store to itself.
///
/// Read-only handles take no lease and may open alongside the writer. A
/// second writable open of a held store waits
/// `UniConfig::writer_lease.acquire_grace`, then fails with
/// `UniError::StoreLocked`. A handle whose lease is lost, or that was
/// closed, refuses writes with `UniError::WriterFenced`.
#[non_exhaustive]
pub enum WriteLease {
    /// The default. A store in a local directory is held by an OS lock on
    /// `writer.lock`; an object store by a lease object at
    /// `catalog/writer_lease.json`, renewed in the background and taken over
    /// once released or expired. An object store without conditional writes
    /// is refused.
    Local,
    /// DynamoDB-based distributed lease. Not implemented: the open fails.
    DynamoDB { table: String },
    /// A custom provider: `acquire` before the first write, `heartbeat` every
    /// `UniConfig::writer_lease.renew_interval` (a failed heartbeat stops
    /// writes), `release` when the handle closes.
    Custom(Box<dyn WriteLeaseProvider>),
    /// No lease or lock: one writer by convention. Logs a warning.
    Unenforced,
}

/// Guard representing an acquired write lease.
pub struct LeaseGuard {
    /// Unique ID for this lease acquisition.
    pub lease_id: String,
    /// When the lease expires (must be renewed via heartbeat before expiry).
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// Trait for custom write lease providers.
///
/// Implement this trait to provide distributed write coordination
/// (e.g., via DynamoDB, etcd, Redis, ZooKeeper).
#[async_trait::async_trait]
pub trait WriteLeaseProvider: Send + Sync {
    /// Attempt to acquire the write lease. Returns `Err` if the lease is held by another writer.
    async fn acquire(&self) -> Result<LeaseGuard>;

    /// Renew the lease before it expires.
    async fn heartbeat(&self, guard: &LeaseGuard) -> Result<()>;

    /// Release the lease. Called on graceful shutdown.
    async fn release(&self, guard: LeaseGuard) -> Result<()>;
}

impl std::fmt::Debug for WriteLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteLease::Local => write!(f, "WriteLease::Local"),
            WriteLease::DynamoDB { table } => {
                write!(f, "WriteLease::DynamoDB {{ table: {} }}", table)
            }
            WriteLease::Custom(_) => write!(f, "WriteLease::Custom(...)"),
            WriteLease::Unenforced => write!(f, "WriteLease::Unenforced"),
        }
    }
}
