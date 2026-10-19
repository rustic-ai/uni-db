// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Consolidated integration-test binary for `uni-tck`.
//!
//! Cargo defaults to one binary per `.rs` file in `tests/`. `autotests = false`
//! in `Cargo.toml` disables that auto-discovery; this `[[test]]` entry pulls
//! every sibling repro in as a module. The cucumber runner (`tests/tck.rs`)
//! stays separate because it is `harness = false`.
//!
//! Adding a repro? Add a `mod` line here — do not create a new top-level
//! `tests/*.rs`. See `docs/test_layout.md`.

// The `collect_ids` swallowed-error repro lives as unit tests in
// `src/world.rs`: the helpers are private, and only an in-module test can hand
// them a failing query.
// A scenario asserting side effects passed even when none were produced.
mod repro_side_effects_false_pass;
// Value sort key ordered nodes and lists inconsistently with the TCK.
mod repro_value_sort_key_node_list;
