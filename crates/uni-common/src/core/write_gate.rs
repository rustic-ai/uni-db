// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Whether a handle may still write its store.
//!
//! Implemented by the storage layer's writer fence; catalog writers in this
//! crate (the schema) consult it without depending on the storage crate.

/// A gate every catalog write passes.
pub trait WriteGate: Send + Sync + std::fmt::Debug {
    /// Fails once the handle may no longer write.
    ///
    /// # Errors
    /// The handle lost its writer lease, or was closed.
    fn check(&self) -> anyhow::Result<()>;

    /// Records that another writer is active; the handle stops writing.
    fn trip(&self, reason: String);
}
