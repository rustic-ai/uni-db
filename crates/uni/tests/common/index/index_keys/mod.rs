// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Index keys: scalar indexes on nested and opaque property types through
//! derived key columns (docs/proposals/index_keys_2026_11_23.md).

pub mod btic_contains_point;
pub mod btic_differential;
pub mod btic_lifecycle;
pub mod datetime_bulk_clock_types;
pub mod datetime_differential;
pub mod datetime_hidden_and_forks;
pub mod datetime_lifecycle;
pub mod empty_table_key_columns;
pub mod fork_table_rewrite;
pub mod harness;
pub mod index_name_and_key_paths;
pub mod keyed_pushdown_views;
pub mod map_path_differential;
pub mod null_key_superset;
pub mod point_differential;
pub mod refusal_pointers;
pub mod rewrite_keeps_indexes;
pub mod time_differential;
pub mod uid_derived_columns;
pub mod upgrade_outside_flush;
