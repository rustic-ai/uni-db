// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Canonical datetime parsing shared across the workspace.
//!
//! This lives in `uni-common` rather than beside the other temporal helpers in
//! `uni-query-functions` because the storage layer needs it too, and
//! `uni-query-functions` depends on `uni-store` — putting it there would make
//! `uni-store` -> `uni-query-functions` a dependency cycle. It is pure chrono,
//! so the lowest crate is a natural home.

use anyhow::{Result, anyhow};
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};

/// Parse a datetime string to UTC.
///
/// Supports multiple formats:
/// - RFC3339 (e.g., "2023-01-01T00:00:00Z")
/// - "%Y-%m-%d %H:%M:%S %z" (e.g., "2023-01-01 00:00:00 +0000")
/// - "%Y-%m-%d %H:%M:%S" naive (assumed UTC)
///
/// This is the canonical datetime parsing function for temporal operations
/// like `validAt`. Using a single implementation ensures consistent behavior.
///
/// # Errors
///
/// Returns an error when the string matches none of the supported formats.
pub fn parse_datetime_utc(s: &str) -> Result<DateTime<Utc>> {
    // Temporal string renderings in the engine can include a bracketed timezone
    // suffix (e.g. "2020-01-01T00:00Z[UTC]"). Strip it for parsing while keeping
    // the explicit offset/UTC marker in the base datetime.
    let s = s.trim();
    let parse_input = match s.rfind('[') {
        Some(pos) if s.ends_with(']') => &s[..pos],
        _ => s,
    };

    DateTime::parse_from_rfc3339(parse_input)
        .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
        .or_else(|_| {
            // Handle formats without seconds (e.g., "2023-01-01T00:00Z")
            if let Some(base) = parse_input.strip_suffix('Z') {
                NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M")
                    .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
            } else {
                // Handle formats without seconds with offset (e.g., "2023-01-01T00:00+05:00")
                DateTime::parse_from_str(parse_input, "%Y-%m-%dT%H:%M%:z")
                    .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
            }
        })
        .or_else(|_| {
            DateTime::parse_from_str(parse_input, "%Y-%m-%d %H:%M:%S %z")
                .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(parse_input, "%Y-%m-%d %H:%M:%S")
                .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
        })
        .map_err(|_| anyhow!("Invalid datetime format: {}", s))
}
