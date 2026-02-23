// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Typed value representation for graph properties and query results.
//!
//! [`Value`] is the canonical internal representation for all property values,
//! query parameters, and expression results. Unlike `serde_json::Value`, it
//! distinguishes integers from floats (`Int(i64)` vs `Float(f64)`) and includes
//! graph-specific variants (`Node`, `Edge`, `Path`, `Vector`).
//!
//! Conversion to/from `serde_json::Value` is provided at the serialization
//! boundary via `From` implementations.

use crate::api::error::UniError;
use crate::core::id::{Eid, Vid};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};

// ============================================================================
// Temporal Value Types
// ============================================================================

/// Classification of temporal types for dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TemporalType {
    Date,
    LocalTime,
    Time,
    LocalDateTime,
    DateTime,
    Duration,
}

/// Typed temporal value representation.
///
/// Stores temporal values in their native numeric form for O(1) comparisons
/// and direct Arrow column construction, with Cypher formatting applied only
/// at the output boundary via [`std::fmt::Display`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TemporalValue {
    /// Date: days since Unix epoch (1970-01-01). Arrow: Date32.
    Date { days_since_epoch: i32 },
    /// Local time (no timezone): nanoseconds since midnight. Arrow: Time64(ns).
    LocalTime { nanos_since_midnight: i64 },
    /// Time with timezone offset: nanoseconds since midnight + offset. Arrow: Time64(ns) + metadata.
    Time {
        nanos_since_midnight: i64,
        offset_seconds: i32,
    },
    /// Local datetime (no timezone): nanoseconds since Unix epoch. Arrow: Timestamp(ns, None).
    LocalDateTime { nanos_since_epoch: i64 },
    /// Datetime with timezone: nanoseconds since Unix epoch (UTC) + offset + optional tz name.
    /// Arrow: Timestamp(ns, Some("UTC")).
    DateTime {
        nanos_since_epoch: i64,
        offset_seconds: i32,
        timezone_name: Option<String>,
    },
    /// Duration with calendar semantics: months + days + nanoseconds.
    /// Matches Cypher's duration model which preserves calendar components.
    Duration { months: i64, days: i64, nanos: i64 },
}

impl Eq for TemporalValue {}

impl Hash for TemporalValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            TemporalValue::Date { days_since_epoch } => days_since_epoch.hash(state),
            TemporalValue::LocalTime {
                nanos_since_midnight,
            } => nanos_since_midnight.hash(state),
            TemporalValue::Time {
                nanos_since_midnight,
                offset_seconds,
            } => {
                nanos_since_midnight.hash(state);
                offset_seconds.hash(state);
            }
            TemporalValue::LocalDateTime { nanos_since_epoch } => nanos_since_epoch.hash(state),
            TemporalValue::DateTime {
                nanos_since_epoch,
                offset_seconds,
                timezone_name,
            } => {
                nanos_since_epoch.hash(state);
                offset_seconds.hash(state);
                timezone_name.hash(state);
            }
            TemporalValue::Duration {
                months,
                days,
                nanos,
            } => {
                months.hash(state);
                days.hash(state);
                nanos.hash(state);
            }
        }
    }
}

impl TemporalValue {
    /// Returns the temporal type classification.
    pub fn temporal_type(&self) -> TemporalType {
        match self {
            TemporalValue::Date { .. } => TemporalType::Date,
            TemporalValue::LocalTime { .. } => TemporalType::LocalTime,
            TemporalValue::Time { .. } => TemporalType::Time,
            TemporalValue::LocalDateTime { .. } => TemporalType::LocalDateTime,
            TemporalValue::DateTime { .. } => TemporalType::DateTime,
            TemporalValue::Duration { .. } => TemporalType::Duration,
        }
    }

    // -----------------------------------------------------------------------
    // Component accessors
    // -----------------------------------------------------------------------

    /// Year component, or None for time-only/duration types.
    pub fn year(&self) -> Option<i64> {
        self.to_date().map(|d| d.year() as i64)
    }

    /// Month component (1-12), or None for time-only/duration types.
    pub fn month(&self) -> Option<i64> {
        self.to_date().map(|d| d.month() as i64)
    }

    /// Day-of-month component (1-31), or None for time-only/duration types.
    pub fn day(&self) -> Option<i64> {
        self.to_date().map(|d| d.day() as i64)
    }

    /// Hour component (0-23), or None for date-only types.
    pub fn hour(&self) -> Option<i64> {
        self.to_time().map(|t| t.hour() as i64)
    }

    /// Minute component (0-59), or None for date-only types.
    pub fn minute(&self) -> Option<i64> {
        self.to_time().map(|t| t.minute() as i64)
    }

    /// Second component (0-59), or None for date-only types.
    pub fn second(&self) -> Option<i64> {
        self.to_time().map(|t| t.second() as i64)
    }

    /// Millisecond sub-second component (0-999), or None for date-only types.
    pub fn millisecond(&self) -> Option<i64> {
        self.to_time().map(|t| (t.nanosecond() / 1_000_000) as i64)
    }

    /// Microsecond sub-second component (0-999_999), or None for date-only types.
    pub fn microsecond(&self) -> Option<i64> {
        self.to_time().map(|t| (t.nanosecond() / 1_000) as i64)
    }

    /// Nanosecond sub-second component (0-999_999_999), or None for date-only types.
    pub fn nanosecond(&self) -> Option<i64> {
        self.to_time().map(|t| t.nanosecond() as i64)
    }

    /// Quarter (1-4), or None for time-only/duration types.
    pub fn quarter(&self) -> Option<i64> {
        self.to_date().map(|d| ((d.month() - 1) / 3 + 1) as i64)
    }

    /// ISO week number (1-53), or None for time-only/duration types.
    pub fn week(&self) -> Option<i64> {
        self.to_date().map(|d| d.iso_week().week() as i64)
    }

    /// ISO week year, or None for time-only/duration types.
    pub fn week_year(&self) -> Option<i64> {
        self.to_date().map(|d| d.iso_week().year() as i64)
    }

    /// Ordinal day of year (1-366), or None for time-only/duration types.
    pub fn ordinal_day(&self) -> Option<i64> {
        self.to_date().map(|d| d.ordinal() as i64)
    }

    /// ISO day of week (Monday=1, Sunday=7), or None for time-only/duration types.
    pub fn day_of_week(&self) -> Option<i64> {
        self.to_date()
            .map(|d| (d.weekday().num_days_from_monday() + 1) as i64)
    }

    /// Day of quarter (1-92), or None for time-only/duration types.
    pub fn day_of_quarter(&self) -> Option<i64> {
        self.to_date().map(|d| {
            let quarter_start_month = ((d.month() - 1) / 3) * 3 + 1;
            let quarter_start =
                chrono::NaiveDate::from_ymd_opt(d.year(), quarter_start_month, 1).unwrap();
            d.signed_duration_since(quarter_start).num_days() + 1
        })
    }

    /// Timezone name if available (e.g., "Europe/Stockholm").
    pub fn timezone(&self) -> Option<&str> {
        match self {
            TemporalValue::DateTime {
                timezone_name: Some(name),
                ..
            } => Some(name.as_str()),
            _ => None,
        }
    }

    /// Returns the raw offset in seconds for types that carry a timezone offset.
    fn raw_offset_seconds(&self) -> Option<i32> {
        match self {
            TemporalValue::Time { offset_seconds, .. }
            | TemporalValue::DateTime { offset_seconds, .. } => Some(*offset_seconds),
            _ => None,
        }
    }

    /// Offset string (e.g., "+01:00", "Z").
    pub fn offset(&self) -> Option<String> {
        self.raw_offset_seconds().map(format_offset)
    }

    /// Offset in minutes.
    pub fn offset_minutes(&self) -> Option<i64> {
        self.raw_offset_seconds().map(|s| s as i64 / 60)
    }

    /// Offset in seconds.
    pub fn offset_seconds_value(&self) -> Option<i64> {
        self.raw_offset_seconds().map(|s| s as i64)
    }

    /// Returns the raw epoch nanos for types that store nanoseconds since epoch.
    fn raw_epoch_nanos(&self) -> Option<i64> {
        match self {
            TemporalValue::DateTime {
                nanos_since_epoch, ..
            }
            | TemporalValue::LocalDateTime {
                nanos_since_epoch, ..
            } => Some(*nanos_since_epoch),
            _ => None,
        }
    }

    /// Epoch seconds (for datetime/localdatetime types).
    pub fn epoch_seconds(&self) -> Option<i64> {
        self.raw_epoch_nanos().map(|n| n / 1_000_000_000)
    }

    /// Epoch milliseconds (for datetime/localdatetime types).
    pub fn epoch_millis(&self) -> Option<i64> {
        self.raw_epoch_nanos().map(|n| n / 1_000_000)
    }

    // -----------------------------------------------------------------------
    // Internal chrono conversion helpers
    // -----------------------------------------------------------------------

    /// Extract a NaiveDate from types that have a date component.
    pub fn to_date(&self) -> Option<chrono::NaiveDate> {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
        match self {
            TemporalValue::Date { days_since_epoch } => {
                epoch.checked_add_signed(chrono::Duration::days(*days_since_epoch as i64))
            }
            TemporalValue::LocalDateTime { nanos_since_epoch } => {
                let dt = chrono::DateTime::from_timestamp_nanos(*nanos_since_epoch);
                Some(dt.date_naive())
            }
            TemporalValue::DateTime {
                nanos_since_epoch,
                offset_seconds,
                ..
            } => {
                // Convert UTC nanos to local time by adding offset
                let local_nanos = nanos_since_epoch + (*offset_seconds as i64) * 1_000_000_000;
                let dt = chrono::DateTime::from_timestamp_nanos(local_nanos);
                Some(dt.date_naive())
            }
            _ => None,
        }
    }

    /// Extract a NaiveTime from types that have a time component.
    pub fn to_time(&self) -> Option<chrono::NaiveTime> {
        match self {
            TemporalValue::LocalTime {
                nanos_since_midnight,
            }
            | TemporalValue::Time {
                nanos_since_midnight,
                ..
            } => nanos_to_time(*nanos_since_midnight),
            TemporalValue::LocalDateTime { nanos_since_epoch } => {
                let dt = chrono::DateTime::from_timestamp_nanos(*nanos_since_epoch);
                Some(dt.naive_utc().time())
            }
            TemporalValue::DateTime {
                nanos_since_epoch,
                offset_seconds,
                ..
            } => {
                let local_nanos = nanos_since_epoch + (*offset_seconds as i64) * 1_000_000_000;
                let dt = chrono::DateTime::from_timestamp_nanos(local_nanos);
                Some(dt.naive_utc().time())
            }
            _ => None,
        }
    }
}

/// Convert nanoseconds since midnight to NaiveTime.
fn nanos_to_time(nanos: i64) -> Option<chrono::NaiveTime> {
    let total_secs = nanos / 1_000_000_000;
    let h = (total_secs / 3600) as u32;
    let m = ((total_secs % 3600) / 60) as u32;
    let s = (total_secs % 60) as u32;
    let ns = (nanos % 1_000_000_000) as u32;
    chrono::NaiveTime::from_hms_nano_opt(h, m, s, ns)
}

/// Format an offset in seconds as "+HH:MM" or "Z".
fn format_offset(offset_seconds: i32) -> String {
    if offset_seconds == 0 {
        return "Z".to_string();
    }
    format_offset_numeric(offset_seconds)
}

/// Format offset always as `+HH:MM` or `+HH:MM:SS` (never as `Z`).
fn format_offset_numeric(offset_seconds: i32) -> String {
    let sign = if offset_seconds >= 0 { '+' } else { '-' };
    let abs = offset_seconds.unsigned_abs();
    let h = abs / 3600;
    let m = (abs % 3600) / 60;
    let s = abs % 60;
    if s != 0 {
        format!("{}{:02}:{:02}:{:02}", sign, h, m, s)
    } else {
        format!("{}{:02}:{:02}", sign, h, m)
    }
}

/// Format sub-second fractional part, stripping all trailing zeros.
fn format_fractional(nanos: u32) -> String {
    if nanos == 0 {
        return String::new();
    }
    let s = format!("{:09}", nanos);
    let trimmed = s.trim_end_matches('0');
    format!(".{}", trimmed)
}

/// Format time as HH:MM[:SS[.n...]] — omit :SS when seconds and sub-seconds are zero.
fn format_time_component(hour: u32, minute: u32, second: u32, nanos: u32) -> String {
    if second == 0 && nanos == 0 {
        format!("{:02}:{:02}", hour, minute)
    } else {
        let frac = format_fractional(nanos);
        format!("{:02}:{:02}:{:02}{}", hour, minute, second, frac)
    }
}

/// Format a NaiveTime as a canonical time string.
fn format_naive_time(t: &chrono::NaiveTime) -> String {
    format_time_component(t.hour(), t.minute(), t.second(), t.nanosecond())
}

/// Convert nanos since midnight to NaiveTime, defaulting to midnight on invalid input.
fn nanos_to_time_or_midnight(nanos: i64) -> chrono::NaiveTime {
    nanos_to_time(nanos).unwrap_or_else(|| chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
}

impl fmt::Display for TemporalValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TemporalValue::Date { days_since_epoch } => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch + chrono::Duration::days(*days_since_epoch as i64);
                write!(f, "{}", date.format("%Y-%m-%d"))
            }
            TemporalValue::LocalTime {
                nanos_since_midnight,
            } => {
                let time = nanos_to_time_or_midnight(*nanos_since_midnight);
                write!(f, "{}", format_naive_time(&time))
            }
            TemporalValue::Time {
                nanos_since_midnight,
                offset_seconds,
            } => {
                let time = nanos_to_time_or_midnight(*nanos_since_midnight);
                write!(
                    f,
                    "{}{}",
                    format_naive_time(&time),
                    format_offset(*offset_seconds)
                )
            }
            TemporalValue::LocalDateTime { nanos_since_epoch } => {
                let ndt = chrono::DateTime::from_timestamp_nanos(*nanos_since_epoch).naive_utc();
                write!(
                    f,
                    "{}T{}",
                    ndt.date().format("%Y-%m-%d"),
                    format_naive_time(&ndt.time())
                )
            }
            TemporalValue::DateTime {
                nanos_since_epoch,
                offset_seconds,
                timezone_name,
            } => {
                // Display in local time (UTC nanos + offset)
                let local_nanos = nanos_since_epoch + (*offset_seconds as i64) * 1_000_000_000;
                let ndt = chrono::DateTime::from_timestamp_nanos(local_nanos).naive_utc();
                let tz = format_offset(*offset_seconds);
                write!(
                    f,
                    "{}T{}{}",
                    ndt.date().format("%Y-%m-%d"),
                    format_naive_time(&ndt.time()),
                    tz
                )?;
                if let Some(name) = timezone_name {
                    write!(f, "[{}]", name)?;
                }
                Ok(())
            }
            TemporalValue::Duration {
                months,
                days,
                nanos,
            } => {
                write!(f, "P")?;
                let years = months / 12;
                let rem_months = months % 12;
                if years != 0 {
                    write!(f, "{}Y", years)?;
                }
                if rem_months != 0 {
                    write!(f, "{}M", rem_months)?;
                }
                if *days != 0 {
                    write!(f, "{}D", days)?;
                }
                // Time part
                let abs_nanos = nanos.unsigned_abs() as i128;
                let nanos_sign = if *nanos < 0 { -1i64 } else { 1 };
                let total_secs = (abs_nanos / 1_000_000_000) as i64;
                let frac_nanos = (abs_nanos % 1_000_000_000) as u32;
                let hours = total_secs / 3600;
                let mins = (total_secs % 3600) / 60;
                let secs = total_secs % 60;

                if hours != 0 || mins != 0 || secs != 0 || frac_nanos != 0 {
                    write!(f, "T")?;
                    if hours != 0 {
                        write!(f, "{}H", hours * nanos_sign)?;
                    }
                    if mins != 0 {
                        write!(f, "{}M", mins * nanos_sign)?;
                    }
                    if secs != 0 || frac_nanos != 0 {
                        let frac = format_fractional(frac_nanos);
                        if nanos_sign < 0 && (secs != 0 || frac_nanos != 0) {
                            write!(f, "-{}{}", secs, frac)?;
                        } else {
                            write!(f, "{}{}", secs, frac)?;
                        }
                        write!(f, "S")?;
                    }
                } else if years == 0 && rem_months == 0 && *days == 0 {
                    // Zero duration
                    write!(f, "T0S")?;
                }
                Ok(())
            }
        }
    }
}

// Use chrono traits in component accessors - needed by TemporalValue accessors
use chrono::Datelike as _;
use chrono::Timelike as _;

/// Dynamic value type for properties, parameters, and results.
///
/// Preserves the distinction between integers and floats, and includes
/// graph-specific variants for nodes, edges, paths, and vectors.
///
/// Note: `Eq` and `Hash` are implemented manually to support using `Value` as
/// HashMap keys. Floats are compared/hashed by their bit representation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Value {
    /// JSON/Cypher null.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating-point number.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw byte buffer.
    Bytes(Vec<u8>),
    /// Ordered list of values.
    List(Vec<Value>),
    /// String-keyed map of values.
    Map(HashMap<String, Value>),

    // Graph-specific
    /// Graph node with VID, label, and properties.
    Node(Node),
    /// Graph edge with EID, type, endpoints, and properties.
    Edge(Edge),
    /// Graph path (alternating nodes and edges).
    Path(Path),

    // Vector
    /// Dense float vector for similarity search.
    Vector(Vec<f32>),

    // Temporal
    /// Typed temporal value (date, time, datetime, duration).
    Temporal(TemporalValue),
}

// ---------------------------------------------------------------------------
// Accessor methods (mirrors serde_json::Value API for migration ease)
// ---------------------------------------------------------------------------

impl Value {
    /// Returns `true` if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the boolean if this is `Bool`, otherwise `None`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns the integer if this is `Int`, otherwise `None`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Returns the integer as `u64` if this is a non-negative `Int`, otherwise `None`.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Int(i) if *i >= 0 => Some(*i as u64),
            _ => None,
        }
    }

    /// Returns a float, coercing `Int` to `f64` if needed.
    ///
    /// Returns `None` for non-numeric variants.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Returns the string slice if this is `String`, otherwise `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns `true` if this is `Int`.
    pub fn is_i64(&self) -> bool {
        matches!(self, Value::Int(_))
    }

    /// Returns `true` if this is `Float` (not `Int`).
    pub fn is_f64(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    /// Returns `true` if this is `String`.
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Returns `true` if this is `Int` or `Float`.
    pub fn is_number(&self) -> bool {
        matches!(self, Value::Int(_) | Value::Float(_))
    }

    /// Returns the list if this is `List`, otherwise `None`.
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    /// Returns the map if this is `Map`, otherwise `None`.
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Returns `true` if this is `Bool`.
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Returns `true` if this is `List`.
    pub fn is_list(&self) -> bool {
        matches!(self, Value::List(_))
    }

    /// Returns `true` if this is `Map`.
    pub fn is_map(&self) -> bool {
        matches!(self, Value::Map(_))
    }

    /// Gets a value by key if this is a `Map`.
    ///
    /// Returns `None` if not a map or key doesn't exist.
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(m) => m.get(key),
            _ => None,
        }
    }

    /// Returns `true` if this is a `Temporal` value.
    pub fn is_temporal(&self) -> bool {
        matches!(self, Value::Temporal(_))
    }

    /// Returns the temporal value reference if this is `Temporal`, otherwise `None`.
    pub fn as_temporal(&self) -> Option<&TemporalValue> {
        match self {
            Value::Temporal(t) => Some(t),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(v) => {
                if v.fract() == 0.0 && v.is_finite() {
                    write!(f, "{v:.1}")
                } else {
                    write!(f, "{v}")
                }
            }
            Value::String(s) => write!(f, "{s}"),
            Value::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            Value::List(l) => {
                write!(f, "[")?;
                for (i, item) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Value::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            Value::Node(n) => write!(f, "(:{} {{vid: {}}})", n.labels.join(":"), n.vid),
            Value::Edge(e) => write!(f, "-[:{}]-", e.edge_type),
            Value::Path(p) => write!(
                f,
                "<path: {} nodes, {} edges>",
                p.nodes.len(),
                p.edges.len()
            ),
            Value::Vector(v) => write!(f, "<vector: {} dims>", v.len()),
            Value::Temporal(t) => write!(f, "{t}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Eq and Hash implementations
// ---------------------------------------------------------------------------

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Discriminant first for type safety
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::Bytes(b) => b.hash(state),
            Value::List(l) => l.hash(state),
            Value::Map(m) => hash_map(m, state),
            Value::Node(n) => n.hash(state),
            Value::Edge(e) => e.hash(state),
            Value::Path(p) => p.hash(state),
            Value::Vector(v) => {
                v.len().hash(state);
                for f in v {
                    f.to_bits().hash(state);
                }
            }
            Value::Temporal(t) => t.hash(state),
        }
    }
}

// ---------------------------------------------------------------------------
// Graph entity types
// ---------------------------------------------------------------------------

/// Helper to hash a HashMap deterministically by sorting keys.
fn hash_map<H: Hasher>(m: &HashMap<String, Value>, state: &mut H) {
    let mut pairs: Vec<_> = m.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);
    pairs.len().hash(state);
    for (k, v) in pairs {
        k.hash(state);
        v.hash(state);
    }
}

/// Graph node with identity, labels, and properties.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    /// Internal vertex identifier.
    pub vid: Vid,
    /// Node labels (multi-label support).
    pub labels: Vec<String>,
    /// Property key-value pairs.
    pub properties: HashMap<String, Value>,
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.vid.hash(state);
        let mut sorted_labels = self.labels.clone();
        sorted_labels.sort();
        sorted_labels.hash(state);
        hash_map(&self.properties, state);
    }
}

impl Node {
    /// Gets a typed property by name.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Query` if the property is missing,
    /// or `UniError::Type` if it cannot be converted.
    pub fn get<T: FromValue>(&self, property: &str) -> crate::Result<T> {
        let val = self
            .properties
            .get(property)
            .ok_or_else(|| UniError::Query {
                message: format!("Property '{}' not found on node {}", property, self.vid),
                query: None,
            })?;
        T::from_value(val)
    }

    /// Tries to get a typed property, returning `None` on failure.
    pub fn try_get<T: FromValue>(&self, property: &str) -> Option<T> {
        self.properties
            .get(property)
            .and_then(|v| T::from_value(v).ok())
    }
}

/// Graph edge with identity, type, endpoints, and properties.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    /// Internal edge identifier.
    pub eid: Eid,
    /// Relationship type name.
    pub edge_type: String,
    /// Source vertex ID.
    pub src: Vid,
    /// Destination vertex ID.
    pub dst: Vid,
    /// Property key-value pairs.
    pub properties: HashMap<String, Value>,
}

impl Hash for Edge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.eid.hash(state);
        self.edge_type.hash(state);
        self.src.hash(state);
        self.dst.hash(state);
        hash_map(&self.properties, state);
    }
}

impl Edge {
    /// Gets a typed property by name.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Query` if the property is missing,
    /// or `UniError::Type` if it cannot be converted.
    pub fn get<T: FromValue>(&self, property: &str) -> crate::Result<T> {
        let val = self
            .properties
            .get(property)
            .ok_or_else(|| UniError::Query {
                message: format!("Property '{}' not found on edge {}", property, self.eid),
                query: None,
            })?;
        T::from_value(val)
    }
}

/// Graph path consisting of alternating nodes and edges.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Path {
    /// Ordered sequence of nodes along the path.
    pub nodes: Vec<Node>,
    /// Ordered sequence of edges connecting the nodes.
    #[serde(rename = "relationships")]
    pub edges: Vec<Edge>,
}

impl Path {
    /// Returns the nodes in this path.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Returns the edges in this path.
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    /// Returns the number of edges (path length).
    pub fn len(&self) -> usize {
        self.edges.len()
    }

    /// Returns `true` if the path has no edges.
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Returns the starting node, or `None` if the path is empty.
    pub fn start(&self) -> Option<&Node> {
        self.nodes.first()
    }

    /// Returns the ending node, or `None` if the path is empty.
    pub fn end(&self) -> Option<&Node> {
        self.nodes.last()
    }
}

// ---------------------------------------------------------------------------
// FromValue trait
// ---------------------------------------------------------------------------

/// Trait for fallible conversion from [`Value`].
pub trait FromValue: Sized {
    /// Converts a `Value` reference to `Self`.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Type` if the value cannot be converted.
    fn from_value(value: &Value) -> crate::Result<Self>;
}

/// Blanket implementation: any `T: TryFrom<&Value, Error = UniError>` is `FromValue`.
impl<T> FromValue for T
where
    T: for<'a> TryFrom<&'a Value, Error = UniError>,
{
    fn from_value(value: &Value) -> crate::Result<Self> {
        Self::try_from(value)
    }
}

// ---------------------------------------------------------------------------
// TryFrom<Value> macro for owned values (delegates to &Value)
// ---------------------------------------------------------------------------

macro_rules! impl_try_from_value_owned {
    ($($t:ty),+ $(,)?) => {
        $(
            impl TryFrom<Value> for $t {
                type Error = UniError;
                fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
                    Self::try_from(&value)
                }
            }
        )+
    };
}

impl_try_from_value_owned!(
    String,
    i64,
    i32,
    f64,
    bool,
    Vid,
    Eid,
    Vec<f32>,
    Path,
    Node,
    Edge
);

// ---------------------------------------------------------------------------
// TryFrom<&Value> implementations for standard types
// ---------------------------------------------------------------------------

/// Create a type mismatch error.
fn type_error(expected: &str, value: &Value) -> UniError {
    UniError::Type {
        expected: expected.to_string(),
        actual: format!("{:?}", value),
    }
}

impl TryFrom<&Value> for String {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(s.clone()),
            Value::Int(i) => Ok(i.to_string()),
            Value::Float(f) => Ok(f.to_string()),
            Value::Bool(b) => Ok(b.to_string()),
            Value::Temporal(t) => Ok(t.to_string()),
            _ => Err(type_error("String", value)),
        }
    }
}

impl TryFrom<&Value> for i64 {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Int(i) => Ok(*i),
            Value::Float(f) => Ok(*f as i64),
            _ => Err(type_error("Int", value)),
        }
    }
}

impl TryFrom<&Value> for i32 {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Int(i) => i32::try_from(*i).map_err(|_| UniError::Type {
                expected: "i32".to_string(),
                actual: format!("Integer {} out of range", i),
            }),
            Value::Float(f) => {
                if *f < i32::MIN as f64 || *f > i32::MAX as f64 {
                    return Err(UniError::Type {
                        expected: "i32".to_string(),
                        actual: format!("Float {} out of range", f),
                    });
                }
                if f.fract() != 0.0 {
                    return Err(UniError::Type {
                        expected: "i32".to_string(),
                        actual: format!("Float {} has fractional part", f),
                    });
                }
                Ok(*f as i32)
            }
            _ => Err(type_error("Int", value)),
        }
    }
}

impl TryFrom<&Value> for f64 {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            _ => Err(type_error("Float", value)),
        }
    }
}

impl TryFrom<&Value> for bool {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Bool(b) => Ok(*b),
            _ => Err(type_error("Bool", value)),
        }
    }
}

impl TryFrom<&Value> for Vid {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Node(n) => Ok(n.vid),
            Value::String(s) => {
                if let Ok(id) = s.parse::<u64>() {
                    return Ok(Vid::new(id));
                }
                Err(UniError::Type {
                    expected: "Vid".into(),
                    actual: s.clone(),
                })
            }
            Value::Int(i) => Ok(Vid::new(*i as u64)),
            _ => Err(type_error("Vid", value)),
        }
    }
}

impl TryFrom<&Value> for Eid {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Edge(e) => Ok(e.eid),
            Value::String(s) => {
                if let Ok(id) = s.parse::<u64>() {
                    return Ok(Eid::new(id));
                }
                Err(UniError::Type {
                    expected: "Eid".into(),
                    actual: s.clone(),
                })
            }
            Value::Int(i) => Ok(Eid::new(*i as u64)),
            _ => Err(type_error("Eid", value)),
        }
    }
}

impl TryFrom<&Value> for Vec<f32> {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Vector(v) => Ok(v.clone()),
            Value::List(l) => {
                let mut vec = Vec::with_capacity(l.len());
                for item in l {
                    match item {
                        Value::Float(f) => vec.push(*f as f32),
                        Value::Int(i) => vec.push(*i as f32),
                        _ => return Err(type_error("Float", item)),
                    }
                }
                Ok(vec)
            }
            _ => Err(type_error("Vector", value)),
        }
    }
}

impl<T> TryFrom<&Value> for Option<T>
where
    T: for<'a> TryFrom<&'a Value, Error = UniError>,
{
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(None),
            _ => T::try_from(value).map(Some),
        }
    }
}

impl<T> TryFrom<Value> for Option<T>
where
    T: TryFrom<Value, Error = UniError>,
{
    type Error = UniError;
    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(None),
            _ => T::try_from(value).map(Some),
        }
    }
}

impl<T> TryFrom<&Value> for Vec<T>
where
    T: for<'a> TryFrom<&'a Value, Error = UniError>,
{
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::List(l) => {
                let mut vec = Vec::with_capacity(l.len());
                for item in l {
                    vec.push(T::try_from(item)?);
                }
                Ok(vec)
            }
            _ => Err(type_error("List", value)),
        }
    }
}

impl<T> TryFrom<Value> for Vec<T>
where
    T: TryFrom<Value, Error = UniError>,
{
    type Error = UniError;
    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::List(l) => {
                let mut vec = Vec::with_capacity(l.len());
                for item in l {
                    vec.push(T::try_from(item)?);
                }
                Ok(vec)
            }
            other => Err(type_error("List", &other)),
        }
    }
}

// ---------------------------------------------------------------------------
// TryFrom<&Value> for graph entities (deserialization from Map)
// ---------------------------------------------------------------------------

/// Gets a value from a map trying alternative keys in order.
fn get_with_fallback<'a>(map: &'a HashMap<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|k| map.get(*k))
}

/// Extracts a properties map from a value, defaulting to empty.
fn extract_properties(value: &Value) -> HashMap<String, Value> {
    match value {
        Value::Map(m) => m.clone(),
        _ => HashMap::new(),
    }
}

impl TryFrom<&Value> for Node {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Node(n) => Ok(n.clone()),
            Value::Map(m) => {
                let vid_val = get_with_fallback(m, &["_vid", "_id", "vid"]);
                let props_val = m.get("properties");

                let (Some(v), Some(p)) = (vid_val, props_val) else {
                    return Err(type_error("Node Map", value));
                };

                // Extract labels from _labels key (List<String>)
                let labels = if let Some(Value::List(label_list)) = m.get("_labels") {
                    label_list
                        .iter()
                        .filter_map(|v| {
                            if let Value::String(s) = v {
                                Some(s.clone())
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                Ok(Node {
                    vid: Vid::try_from(v)?,
                    labels,
                    properties: extract_properties(p),
                })
            }
            _ => Err(type_error("Node", value)),
        }
    }
}

impl TryFrom<&Value> for Edge {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Edge(e) => Ok(e.clone()),
            Value::Map(m) => {
                let eid_val = get_with_fallback(m, &["_eid", "_id", "eid"]);
                let type_val = get_with_fallback(m, &["_type_name", "_type", "edge_type"]);
                let src_val = get_with_fallback(m, &["_src", "src"]);
                let dst_val = get_with_fallback(m, &["_dst", "dst"]);
                let props_val = m.get("properties");

                let (Some(id), Some(t), Some(s), Some(d), Some(p)) =
                    (eid_val, type_val, src_val, dst_val, props_val)
                else {
                    return Err(type_error("Edge Map", value));
                };

                Ok(Edge {
                    eid: Eid::try_from(id)?,
                    edge_type: String::try_from(t)?,
                    src: Vid::try_from(s)?,
                    dst: Vid::try_from(d)?,
                    properties: extract_properties(p),
                })
            }
            _ => Err(type_error("Edge", value)),
        }
    }
}

impl TryFrom<&Value> for Path {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Path(p) => Ok(p.clone()),
            Value::Map(m) => {
                let (Some(Value::List(nodes_list)), Some(Value::List(rels_list))) =
                    (m.get("nodes"), m.get("relationships"))
                else {
                    return Err(type_error("Path (Map with nodes/relationships)", value));
                };

                let nodes = nodes_list
                    .iter()
                    .map(Node::try_from)
                    .collect::<std::result::Result<Vec<_>, _>>()?;

                let edges = rels_list
                    .iter()
                    .map(Edge::try_from)
                    .collect::<std::result::Result<Vec<_>, _>>()?;

                Ok(Path { nodes, edges })
            }
            _ => Err(type_error("Path", value)),
        }
    }
}

// ---------------------------------------------------------------------------
// From<T> for Value (primitive constructors)
// ---------------------------------------------------------------------------

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::Vector(v)
    }
}

// ---------------------------------------------------------------------------
// serde_json::Value ↔ Value conversions (JSONB boundary)
// ---------------------------------------------------------------------------

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::List(arr.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(obj) => {
                Value::Map(obj.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Int(i) => serde_json::Value::Number(serde_json::Number::from(i)),
            Value::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null), // NaN/Inf → null
            Value::String(s) => serde_json::Value::String(s),
            Value::Bytes(b) => {
                use base64::Engine;
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
            }
            Value::List(l) => {
                serde_json::Value::Array(l.into_iter().map(serde_json::Value::from).collect())
            }
            Value::Map(m) => {
                let mut map = serde_json::Map::new();
                for (k, v) in m {
                    map.insert(k, v.into());
                }
                serde_json::Value::Object(map)
            }
            Value::Node(n) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "_id".to_string(),
                    serde_json::Value::String(n.vid.to_string()),
                );
                map.insert(
                    "_labels".to_string(),
                    serde_json::Value::Array(
                        n.labels
                            .into_iter()
                            .map(serde_json::Value::String)
                            .collect(),
                    ),
                );
                let props: serde_json::Value = Value::Map(n.properties).into();
                map.insert("properties".to_string(), props);
                serde_json::Value::Object(map)
            }
            Value::Edge(e) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "_id".to_string(),
                    serde_json::Value::String(e.eid.to_string()),
                );
                map.insert("_type".to_string(), serde_json::Value::String(e.edge_type));
                map.insert(
                    "_src".to_string(),
                    serde_json::Value::String(e.src.to_string()),
                );
                map.insert(
                    "_dst".to_string(),
                    serde_json::Value::String(e.dst.to_string()),
                );
                let props: serde_json::Value = Value::Map(e.properties).into();
                map.insert("properties".to_string(), props);
                serde_json::Value::Object(map)
            }
            Value::Path(p) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "nodes".to_string(),
                    Value::List(p.nodes.into_iter().map(Value::Node).collect()).into(),
                );
                map.insert(
                    "relationships".to_string(),
                    Value::List(p.edges.into_iter().map(Value::Edge).collect()).into(),
                );
                serde_json::Value::Object(map)
            }
            Value::Vector(v) => serde_json::Value::Array(
                v.into_iter()
                    .map(|f| {
                        serde_json::Number::from_f64(f as f64)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect(),
            ),
            Value::Temporal(t) => serde_json::Value::String(t.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// unival! macro
// ---------------------------------------------------------------------------

/// Constructs a [`Value`] from a literal or expression, similar to `serde_json::json!`.
///
/// # Examples
///
/// ```
/// use uni_common::unival;
/// use uni_common::Value;
///
/// let null = unival!(null);
/// let b = unival!(true);
/// let i = unival!(42);
/// let f = unival!(3.14);
/// let s = unival!("hello");
/// let list = unival!([1, 2, "three"]);
/// let map = unival!({"key": "val", "num": 42});
/// let expr_val = { let x: i64 = 10; unival!(x) };
/// ```
#[macro_export]
macro_rules! unival {
    // Null
    (null) => {
        $crate::Value::Null
    };

    // Booleans
    (true) => {
        $crate::Value::Bool(true)
    };
    (false) => {
        $crate::Value::Bool(false)
    };

    // Array
    ([ $($elem:tt),* $(,)? ]) => {
        $crate::Value::List(vec![ $( $crate::unival!($elem) ),* ])
    };

    // Map
    ({ $($key:tt : $val:tt),* $(,)? }) => {
        $crate::Value::Map({
            #[allow(unused_mut)]
            let mut map = ::std::collections::HashMap::new();
            $( map.insert(($key).to_string(), $crate::unival!($val)); )*
            map
        })
    };

    // Fallback: any expression — uses From<T> for Value
    ($e:expr) => {
        $crate::Value::from($e)
    };
}

// ---------------------------------------------------------------------------
// Additional From impls for unival! convenience
// ---------------------------------------------------------------------------

impl From<usize> for Value {
    fn from(v: usize) -> Self {
        Value::Int(v as i64)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Int(v as i64)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(v as f64)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accessor_methods() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int(1).is_null());

        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Int(42).as_bool(), None);

        assert_eq!(Value::Int(42).as_i64(), Some(42));
        assert_eq!(Value::Float(2.5).as_i64(), None);

        // as_f64 coerces Int to Float
        assert_eq!(Value::Float(2.5).as_f64(), Some(2.5));
        assert_eq!(Value::Int(42).as_f64(), Some(42.0));
        assert_eq!(Value::String("x".into()).as_f64(), None);

        assert_eq!(Value::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(Value::Int(1).as_str(), None);

        assert!(Value::Int(1).is_i64());
        assert!(!Value::Float(1.0).is_i64());

        assert!(Value::Float(1.0).is_f64());
        assert!(!Value::Int(1).is_f64());

        assert!(Value::Int(1).is_number());
        assert!(Value::Float(1.0).is_number());
        assert!(!Value::String("x".into()).is_number());
    }

    #[test]
    fn test_serde_json_roundtrip() {
        let val = Value::Int(42);
        let json: serde_json::Value = val.clone().into();
        let back: Value = json.into();
        assert_eq!(val, back);

        let val = Value::Float(2.5);
        let json: serde_json::Value = val.clone().into();
        let back: Value = json.into();
        assert_eq!(val, back);

        let val = Value::String("hello".into());
        let json: serde_json::Value = val.clone().into();
        let back: Value = json.into();
        assert_eq!(val, back);

        let val = Value::List(vec![Value::Int(1), Value::Int(2)]);
        let json: serde_json::Value = val.clone().into();
        let back: Value = json.into();
        assert_eq!(val, back);
    }

    #[test]
    fn test_unival_macro() {
        assert_eq!(unival!(null), Value::Null);
        assert_eq!(unival!(true), Value::Bool(true));
        assert_eq!(unival!(false), Value::Bool(false));
        assert_eq!(unival!(42_i64), Value::Int(42));
        assert_eq!(unival!(2.5_f64), Value::Float(2.5));
        assert_eq!(unival!("hello"), Value::String("hello".into()));

        // Array
        let list = unival!([1_i64, 2_i64]);
        assert_eq!(list, Value::List(vec![Value::Int(1), Value::Int(2)]));

        // Map
        let map = unival!({"key": "val", "num": 42_i64});
        if let Value::Map(m) = &map {
            assert_eq!(m.get("key"), Some(&Value::String("val".into())));
            assert_eq!(m.get("num"), Some(&Value::Int(42)));
        } else {
            panic!("Expected Map");
        }

        // Expression fallback
        let x: i64 = 99;
        assert_eq!(unival!(x), Value::Int(99));
    }

    #[test]
    fn test_int_float_distinction_preserved() {
        // This is the key property: Int stays Int, Float stays Float
        let int_val = Value::Int(42);
        let float_val = Value::Float(42.0);

        assert!(int_val.is_i64());
        assert!(!int_val.is_f64());

        assert!(float_val.is_f64());
        assert!(!float_val.is_i64());

        // They are NOT equal (different variants)
        assert_ne!(int_val, float_val);
    }

    #[test]
    fn test_temporal_display_zero_seconds_omitted() {
        // LocalTime: 12:00 (zero seconds omitted)
        let lt = TemporalValue::LocalTime {
            nanos_since_midnight: 12 * 3600 * 1_000_000_000,
        };
        assert_eq!(lt.to_string(), "12:00");

        // LocalTime: 12:31:14 (non-zero seconds kept)
        let lt2 = TemporalValue::LocalTime {
            nanos_since_midnight: (12 * 3600 + 31 * 60 + 14) * 1_000_000_000,
        };
        assert_eq!(lt2.to_string(), "12:31:14");

        // LocalTime: 00:00:00.5 (zero seconds but non-zero nanos — keep seconds)
        let lt3 = TemporalValue::LocalTime {
            nanos_since_midnight: 500_000_000,
        };
        assert_eq!(lt3.to_string(), "00:00:00.5");

        // Time: 12:00Z (zero offset uses Z)
        let t = TemporalValue::Time {
            nanos_since_midnight: 12 * 3600 * 1_000_000_000,
            offset_seconds: 0,
        };
        assert_eq!(t.to_string(), "12:00Z");

        // Time: 12:31:14+01:00 (non-zero offset)
        let t2 = TemporalValue::Time {
            nanos_since_midnight: (12 * 3600 + 31 * 60 + 14) * 1_000_000_000,
            offset_seconds: 3600,
        };
        assert_eq!(t2.to_string(), "12:31:14+01:00");

        // LocalDateTime: 1984-10-11T12:31 (zero seconds omitted)
        let epoch_nanos = chrono::NaiveDate::from_ymd_opt(1984, 10, 11)
            .unwrap()
            .and_hms_opt(12, 31, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let ldt = TemporalValue::LocalDateTime {
            nanos_since_epoch: epoch_nanos,
        };
        assert_eq!(ldt.to_string(), "1984-10-11T12:31");

        // DateTime: 1984-10-11T12:31+01:00 (zero seconds, with offset)
        let utc_nanos = chrono::NaiveDate::from_ymd_opt(1984, 10, 11)
            .unwrap()
            .and_hms_opt(11, 31, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let dt = TemporalValue::DateTime {
            nanos_since_epoch: utc_nanos,
            offset_seconds: 3600,
            timezone_name: None,
        };
        assert_eq!(dt.to_string(), "1984-10-11T12:31+01:00");

        // DateTime: 2015-07-21T21:40:32.142+01:00 (non-zero seconds with fractional)
        let utc_nanos2 = chrono::NaiveDate::from_ymd_opt(2015, 7, 21)
            .unwrap()
            .and_hms_nano_opt(20, 40, 32, 142_000_000)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let dt2 = TemporalValue::DateTime {
            nanos_since_epoch: utc_nanos2,
            offset_seconds: 3600,
            timezone_name: None,
        };
        assert_eq!(dt2.to_string(), "2015-07-21T21:40:32.142+01:00");

        // DateTime: 1984-10-11T12:31Z (zero offset uses Z)
        let utc_nanos3 = chrono::NaiveDate::from_ymd_opt(1984, 10, 11)
            .unwrap()
            .and_hms_opt(12, 31, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let dt3 = TemporalValue::DateTime {
            nanos_since_epoch: utc_nanos3,
            offset_seconds: 0,
            timezone_name: None,
        };
        assert_eq!(dt3.to_string(), "1984-10-11T12:31Z");
    }

    #[test]
    fn test_temporal_display_fractional_trailing_zeros_stripped() {
        // Full stripping: .9 not .900
        let d = TemporalValue::Duration {
            months: 0,
            days: 0,
            nanos: 900_000_000,
        };
        assert_eq!(d.to_string(), "PT0.9S");

        // Full stripping: .4 not .400
        let d2 = TemporalValue::Duration {
            months: 0,
            days: 0,
            nanos: 400_000_000,
        };
        assert_eq!(d2.to_string(), "PT0.4S");

        // Millisecond precision preserved: .142
        let d3 = TemporalValue::Duration {
            months: 0,
            days: 0,
            nanos: 142_000_000,
        };
        assert_eq!(d3.to_string(), "PT0.142S");

        // Nanosecond precision: .000000001
        let d4 = TemporalValue::Duration {
            months: 0,
            days: 0,
            nanos: 1,
        };
        assert_eq!(d4.to_string(), "PT0.000000001S");
    }

    #[test]
    fn test_temporal_display_offset_second_precision() {
        // Offset with seconds: +02:05:59
        let t = TemporalValue::Time {
            nanos_since_midnight: 12 * 3600 * 1_000_000_000,
            offset_seconds: 2 * 3600 + 5 * 60 + 59,
        };
        assert_eq!(t.to_string(), "12:00+02:05:59");

        // Negative offset with seconds: -02:05:07
        let t2 = TemporalValue::Time {
            nanos_since_midnight: 12 * 3600 * 1_000_000_000,
            offset_seconds: -(2 * 3600 + 5 * 60 + 7),
        };
        assert_eq!(t2.to_string(), "12:00-02:05:07");
    }

    #[test]
    fn test_temporal_display_datetime_with_timezone_name() {
        let utc_nanos = chrono::NaiveDate::from_ymd_opt(1984, 10, 11)
            .unwrap()
            .and_hms_opt(11, 31, 0)
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
        let dt = TemporalValue::DateTime {
            nanos_since_epoch: utc_nanos,
            offset_seconds: 3600,
            timezone_name: Some("Europe/Stockholm".to_string()),
        };
        assert_eq!(dt.to_string(), "1984-10-11T12:31+01:00[Europe/Stockholm]");
    }
}
