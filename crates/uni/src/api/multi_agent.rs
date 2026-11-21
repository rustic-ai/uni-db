// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Multi-agent access — read-only mode and write lease abstraction.
//!
//! Supports multi-reader / single-writer architectures where multiple
//! processes can open the same database for reads, while writes are
//! coordinated through a lease mechanism.

use uni_common::Result;

/// Write lease strategy for multi-agent access.
///
/// **Not enforced yet.** The strategy is recorded and reported (for example
/// in `Uni::write_lease`), but no lease is acquired, renewed or checked: a
/// [`WriteLeaseProvider`]'s methods are never called, and nothing stops a
/// second writable handle on the same store. Run one writable handle per
/// store. Enforcement (a lease object with fencing on object stores, a
/// directory lock locally) is planned.
#[non_exhaustive]
pub enum WriteLease {
    /// Local single-process lock (default behavior — no external coordination).
    Local,
    /// DynamoDB-based distributed lease. Recorded only: not implemented.
    DynamoDB { table: String },
    /// Custom lease provider via trait object. Recorded only: the provider is
    /// not called.
    Custom(Box<dyn WriteLeaseProvider>),
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
        }
    }
}
