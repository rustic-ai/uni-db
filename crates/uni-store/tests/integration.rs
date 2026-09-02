// Consolidated integration-test binary: every test group links into one binary
// to minimize compile/link time. Each group's sources live under tests/common/<group>/.
//
// Feature-gated groups are gated here at the module level (previously each was a
// separate binary carrying a crate-level `#![cfg(...)]`):
//   - storage / fork_recovery require `lance-backend`
//   - ssi_occ_test requires `ssi`
#[path = "common/bugs/mod.rs"]
mod bugs;
#[path = "common/cloud/mod.rs"]
mod cloud;
#[path = "common/cloud_integration_test.rs"]
mod cloud_integration_test;
#[path = "common/crdt/mod.rs"]
mod crdt;
// Failing storage-race regression repros. Gated on `failpoints` (the file
// itself carries `#![cfg(feature = "failpoints")]`) so the production seams it
// drives are compiled in.
// Crash consistency for the compaction path. Gated on `failpoints` (the file
// itself carries the attribute) so the seams it drives are compiled in.
#[cfg(feature = "failpoints")]
#[path = "common/compaction_resilience.rs"]
mod compaction_resilience;

#[cfg(feature = "failpoints")]
#[path = "common/flush_resilience.rs"]
mod flush_resilience;
#[cfg(feature = "lance-backend")]
#[path = "common/fork_recovery/mod.rs"]
mod fork_recovery;
#[path = "common/property/mod.rs"]
mod property;
#[path = "common/ssi_occ_test.rs"]
mod ssi_occ_test;
#[cfg(feature = "lance-backend")]
#[path = "common/storage/mod.rs"]
mod storage;

// --- consolidated from former standalone binaries (see docs/test_layout.md) ---
#[path = "compaction_crdt_registry_dispatch.rs"]
mod compaction_crdt_registry_dispatch;
#[path = "l0_crdt_registry_dispatch.rs"]
mod l0_crdt_registry_dispatch;
#[path = "property_manager_registry_dispatch.rs"]
mod property_manager_registry_dispatch;
