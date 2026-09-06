// Consolidated integration-test binary: every test file links into one
// binary to minimize compile/link time. See docs/test_layout.md.
// Add new integration tests as a `mod` here, NOT as a new tests/*.rs file.

mod bug_233_cdc_feed_holes;
mod bug_cdc_deliver_gap;
mod bug_defer_before_commit_abort;
mod bug_deferral_name_collision;
mod bug_probe_tombstone_skip;
mod bug_scheduler_cancel_persistence;
mod bug_scheduler_duplicate_registration;
