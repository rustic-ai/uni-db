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
    Btic,
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
    /// Binary Temporal Interval Codec: half-open `[lo, hi)` in milliseconds since epoch,
    /// with per-bound granularity and certainty packed in a 64-bit meta word.
    Btic { lo: i64, hi: i64, meta: u64 },
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
            TemporalValue::Btic { lo, hi, meta } => {
                lo.hash(state);
                hi.hash(state);
                meta.hash(state);
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
            TemporalValue::Btic { .. } => TemporalType::Btic,
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
                // Use a checked add (like `to_date`) so an out-of-range
                // `days_since_epoch` degrades gracefully instead of panicking
                // inside `Display`. On overflow, saturate to chrono's
                // representable range so we still render a valid date string.
                let date = epoch
                    .checked_add_signed(chrono::Duration::days(*days_since_epoch as i64))
                    .unwrap_or(if *days_since_epoch >= 0 {
                        chrono::NaiveDate::MAX
                    } else {
                        chrono::NaiveDate::MIN
                    });
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
            TemporalValue::Btic { lo, hi, meta } => match uni_btic::Btic::new(*lo, *hi, *meta) {
                Ok(btic) => write!(f, "{btic}"),
                Err(_) => write!(f, "Btic[lo={lo}, hi={hi}, meta={meta:#x}]"),
            },
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
/// Note: `PartialEq`, `Eq`, and `Hash` are implemented manually to support
/// using `Value` as a HashMap key. The [`Value::Float`] arm uses a *normalized*
/// total ordering rather than raw IEEE-754: `0.0` equals `-0.0`, and `NaN`
/// equals `NaN` (so `Eq`'s reflexivity holds). `Hash` is consistent with this:
/// all zeros hash alike and all NaNs hash alike. All other floats compare and
/// hash by their (canonical) bit representation. This affects only internal
/// bucketing — Cypher `=`/`IN`/`DISTINCT` route through `cypher_eq`, not here.
/// The identity of a graph entity, independent of how a [`Value`] encodes it.
///
/// Produced by [`Value::entity_ref`]. `Copy`, `Eq`, `Hash` and `Ord` so that
/// equality, `DISTINCT`, join keys, `IN` and `ORDER BY` can all answer the
/// identity question through one type instead of each re-deriving it.
///
/// A vertex and an edge that happen to share a number are **not** equal: the
/// variant is part of the identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EntityRef {
    /// A vertex, identified by its `Vid`.
    Vertex(Vid),
    /// An edge, identified by its `Eid`.
    Edge(Eid),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Learned-sparse vector (SPLADE / BGE-M3): two parallel arrays with
    /// strictly-ascending `indices` (term ids) and parallel `values` (weights).
    /// Holds plain fields; reconstruct the [`uni_sparse_vector::SparseVector`]
    /// type only at boundaries (the BTIC split). Real persistence goes through
    /// the explicit codecs, never untagged serde (which would shadow this as a
    /// `Map`).
    SparseVector {
        /// Term ids, strictly ascending (sorted + unique).
        indices: Vec<u32>,
        /// Weights, parallel to `indices`.
        values: Vec<f32>,
    },

    /// Binary/bit vector for Hamming/Jaccard similarity: a packed byte buffer
    /// where each `u8` is one lane of 8 bits. Persistence goes through the
    /// explicit `FixedSizeList<UInt8>` column and codec paths, never untagged
    /// serde (which would shadow it as a `List`/`Bytes`).
    BinaryVector(Vec<u8>),

    // Temporal
    /// Typed temporal value (date, time, datetime, duration).
    Temporal(TemporalValue),
}

// ---------------------------------------------------------------------------
// Accessor methods (mirrors serde_json::Value API for migration ease)
// ---------------------------------------------------------------------------

impl Value {
    /// The vertex id of a value that *is* a graph vertex.
    ///
    /// One definition, because five hand-rolled ones disagreed. A vertex
    /// reaches an expression as either of two encodings — a native
    /// [`Value::Node`], or a [`Value::Map`] that a round-trip flattened it into
    /// — and which one arrives depends on the path, not on the query. Every
    /// site that recognised only one of them silently treated the other as
    /// "not a vertex", which reads downstream as an empty result rather than an
    /// error.
    ///
    /// The map form appears with either key, since the two round-trips spell it
    /// differently: `_vid` from a path/struct column via `arrow_to_json_value`,
    /// `_id` from a `Value::Node` through serde, which renders the id as the
    /// string `"Vid(7)"`.
    ///
    /// Deliberately narrow: a bare [`Value::Int`] is **not** a vertex. It may
    /// well be a vertex id, but "this value is the entity with id 7" and "this
    /// value is the number 7" are different claims, and conflating them lets a
    /// plain integer column match an entity by coincidence. Callers that hold a
    /// raw id want [`Value::coerce_vid`].
    /// The entity this value denotes, whichever of the two encodings it uses.
    ///
    /// A vertex or an edge reaches an expression either natively
    /// ([`Value::Node`] / [`Value::Edge`]) or as a [`Value::Map`] carrying the
    /// id, and nothing in the pipeline enforces which a given path produces.
    /// Hand-rolling the check is what this exists to end: a site that matches
    /// one encoding silently answers "not an entity" for the other, and the
    /// caller reads that as "not equal", "not a duplicate", or "no such row"
    /// rather than as the failure it is.
    ///
    /// Prefer this over reading `_vid`/`_eid` out of a map. Identity, dedup,
    /// join keys and `IN` all want the same question answered the same way, and
    /// [`EntityRef`] is `Copy`, `Eq`, `Hash` and `Ord` so it serves all four.
    ///
    /// # Vertices and edges are not interchangeable
    ///
    /// `_id` is a spelling **both** encodings accept, so a map is read as an
    /// edge when it carries edge structure (an endpoint pair, or a relationship
    /// type) and as a vertex otherwise. Without that split an edge map with
    /// `_id` reads as the vertex of the same number — two different entities
    /// reporting one identity.
    ///
    /// Deliberately narrow, as [`Value::entity_vid`] is: a bare [`Value::Int`]
    /// is not an entity. Callers holding a raw id want [`Value::coerce_vid`].
    #[must_use]
    pub fn entity_ref(&self) -> Option<EntityRef> {
        match self {
            Value::Node(node) => Some(EntityRef::Vertex(node.vid)),
            Value::Edge(edge) => Some(EntityRef::Edge(edge.eid)),
            Value::Map(map) => entity_ref_from_map(map),
            _ => None,
        }
    }

    /// The edge id this value denotes, in either encoding.
    ///
    /// The edge twin of [`Value::entity_vid`]. It did not exist, which is why
    /// every site needing an edge's identity read `_eid` out of a map by hand
    /// and missed [`Value::Edge`], or missed `_eid` entirely.
    #[must_use]
    pub fn entity_eid(&self) -> Option<Eid> {
        match self.entity_ref()? {
            EntityRef::Edge(eid) => Some(eid),
            EntityRef::Vertex(_) => None,
        }
    }

    #[must_use]
    pub fn entity_vid(&self) -> Option<Vid> {
        match self.entity_ref()? {
            EntityRef::Vertex(vid) => Some(vid),
            // An edge is an entity, but it is not a vertex. Returning its eid
            // here would let an edge match a vertex of the same number.
            EntityRef::Edge(_) => None,
        }
    }

    /// The vertex id a value denotes, accepting a raw id as well as an entity.
    ///
    /// [`Value::entity_vid`] first, then a bare integer or a numeric string —
    /// for the call sites that genuinely receive an id rather than an entity
    /// (a single-column CTE working set, a `$vid` parameter). Use it only where
    /// a raw id is a legitimate input; where an entity is expected, the narrow
    /// form is what keeps a stray integer from matching one.
    ///
    /// A negative integer is rejected rather than wrapped: `as_u64` requires a
    /// non-negative value, so `-1` yields `None` instead of `u64::MAX`.
    #[must_use]
    pub fn coerce_vid(&self) -> Option<Vid> {
        if let Some(vid) = self.entity_vid() {
            return Some(vid);
        }
        if let Some(v) = self.as_u64() {
            return Some(Vid::from(v));
        }
        self.as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Vid::from)
    }

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
            Value::SparseVector { indices, .. } => {
                write!(f, "<sparse vector: {} nnz>", indices.len())
            }
            Value::BinaryVector(bytes) => write!(f, "<binary vector: {} lanes>", bytes.len()),
            Value::Temporal(t) => write!(f, "{t}"),
        }
    }
}

// ---------------------------------------------------------------------------
// PartialEq, Eq, and Hash implementations
// ---------------------------------------------------------------------------

/// Exact ordering of an `i64` against an `f64`, without precision loss.
///
/// Casting the integer to `f64` first (the naive approach) collapses distinct
/// `i64` values above `2^53` onto the same float, so `2^53 + 1` would compare
/// *equal* to `2^53.0`. This compares the integer against the float's exact real
/// value instead, so the full `i64` range orders correctly against any finite
/// float and integer/float ties resolve by true magnitude.
///
/// `f` is assumed non-`NaN`; callers that admit `NaN` must handle it beforehand.
///
/// # Examples
/// ```
/// use std::cmp::Ordering;
/// use uni_common::cmp_i64_f64;
/// assert_eq!(cmp_i64_f64(9_007_199_254_740_993, 9_007_199_254_740_992.0), Ordering::Greater);
/// assert_eq!(cmp_i64_f64(2, 2.0), Ordering::Equal);
/// assert_eq!(cmp_i64_f64(1, 1.5), Ordering::Less);
/// ```
pub fn cmp_i64_f64(i: i64, f: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if f.is_infinite() {
        return if f > 0.0 {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let ff = f.floor();
    // 2^63: every i64 is <= i64::MAX = 2^63 - 1 < 2^63 <= f, so the int is Less.
    if ff >= 9_223_372_036_854_775_808.0 {
        return Ordering::Less;
    }
    // -2^63 = i64::MIN: when floor(f) < -2^63 the int is >= i64::MIN > f.
    if ff < -9_223_372_036_854_775_808.0 {
        return Ordering::Greater;
    }
    // `ff` is integral and within [-2^63, 2^63), so this cast is exact.
    let fi = ff as i64;
    match i.cmp(&fi) {
        // Integer parts equal; a positive fractional part makes `f` the larger.
        Ordering::Equal if f > ff => Ordering::Less,
        Ordering::Equal => Ordering::Equal,
        other => other,
    }
}

/// Normalized float equality used by [`Value`]'s `PartialEq`/`Hash`.
///
/// Treats `0.0 == -0.0` and `NaN == NaN`, so that `Value` upholds the std
/// `Hash`/`Eq` contract (`a == b` implies `hash(a) == hash(b)`) and `Eq`'s
/// reflexivity (`NaN == NaN`). All other floats compare via `total_cmp`, which
/// agrees with IEEE-754 `==` on finite, non-zero values.
fn float_eq_normalized(a: f64, b: f64) -> bool {
    a.total_cmp(&b) == std::cmp::Ordering::Equal
        || (a == 0.0 && b == 0.0)
        || (a.is_nan() && b.is_nan())
}

/// `f32` counterpart of [`float_eq_normalized`], used by the `Vector` and
/// `SparseVector` equality arms.
///
/// Treats `0.0 == -0.0` and `NaN == NaN` so that vector-valued [`Value`]s
/// uphold `Eq` reflexivity and stay consistent with [`hash_f32_normalized`]
/// (which normalizes NaN in the `Hash` impl). Without this, a
/// `Vector`/`SparseVector` containing NaN would not equal itself while still
/// hashing identically — silently breaking `HashSet`/`HashMap` dedup.
fn float_eq_normalized_f32(a: f32, b: f32) -> bool {
    a.total_cmp(&b) == std::cmp::Ordering::Equal
        || (a == 0.0 && b == 0.0)
        || (a.is_nan() && b.is_nan())
}

/// Compares two `f32` slices element-wise using [`float_eq_normalized_f32`].
///
/// Length-checked, then per-element normalized comparison — the equality
/// analogue of the normalized hashing done for `Vec<f32>` weights.
fn slice_eq_normalized_f32(a: &[f32], b: &[f32]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| float_eq_normalized_f32(*x, *y))
}

impl PartialEq for Value {
    /// Structural equality, with the [`Value::Float`] arm normalized so that
    /// `0.0 == -0.0` and `NaN == NaN` (see `float_eq_normalized`).
    ///
    /// All non-float arms match the behavior of the former `#[derive(PartialEq)]`
    /// exactly. Container variants (`List`, `Map`, `Node`, `Edge`, `Path`)
    /// recurse through this same impl, so nested floats normalize too.
    fn eq(&self, other: &Self) -> bool {
        // Graph entities compare by identity, in whichever encoding each side
        // happens to use. Structural comparison here was a wrong answer that no
        // site could see: `Node`'s derive compares the vid, the labels *and* the
        // whole property map, so one vertex hydrated with different property
        // sets on either side came back unequal, and the two encodings of one
        // vertex were never equal at all.
        //
        // The comment this impl used to carry — that it affects only internal
        // bucketing because Cypher routes through `cypher_eq` — was not true.
        // `HashSet<Value>` backs `count(DISTINCT …)` and the recursive-CTE
        // cycle-detection set, and Locy's `values_equal` falls through to here,
        // so this equality reaches results.
        match (self.entity_ref(), other.entity_ref()) {
            (Some(a), Some(b)) => return a == b,
            // An entity is never equal to a non-entity.
            (Some(_), None) | (None, Some(_)) => return false,
            (None, None) => {}
        }

        match (self, other) {
            // Normalized float arm — the whole point of this hand-written impl.
            (Value::Float(a), Value::Float(b)) => float_eq_normalized(*a, *b),
            // All other arms reproduce the derived structural equality.
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::List(a), Value::List(b)) => a == b,
            (Value::Map(a), Value::Map(b)) => a == b,
            (Value::Node(a), Value::Node(b)) => a == b,
            (Value::Edge(a), Value::Edge(b)) => a == b,
            (Value::Path(a), Value::Path(b)) => a == b,
            // `Vec<f32>` `==` uses IEEE-754 (`NaN != NaN`), which would break
            // `Eq` reflexivity and disagree with the NaN-normalizing `Hash`
            // impl; compare element-wise with the same normalization instead.
            (Value::Vector(a), Value::Vector(b)) => slice_eq_normalized_f32(a, b),
            (
                Value::SparseVector {
                    indices: i1,
                    values: v1,
                },
                Value::SparseVector {
                    indices: i2,
                    values: v2,
                },
            ) => i1 == i2 && slice_eq_normalized_f32(v1, v2),
            // Exact byte buffers — native `Vec<u8>` equality (no float
            // normalization); parallel to the `Bytes` arm.
            (Value::BinaryVector(a), Value::BinaryVector(b)) => a == b,
            (Value::Temporal(a), Value::Temporal(b)) => a == b,
            // Distinct variants are never equal.
            _ => false,
        }
    }
}

impl Eq for Value {}

/// Hashes an `f64` with signed-zero and NaN normalization.
///
/// `0.0` and `-0.0` hash identically, and every NaN bit pattern hashes
/// identically, keeping `Hash` consistent with [`float_eq_normalized`].
fn hash_f64_normalized<H: Hasher>(f: f64, state: &mut H) {
    let bits = if f == 0.0 {
        0.0f64.to_bits()
    } else if f.is_nan() {
        f64::NAN.to_bits()
    } else {
        f.to_bits()
    };
    bits.hash(state);
}

/// Hashes an `f32` with signed-zero and NaN normalization.
///
/// The `f32` counterpart of [`hash_f64_normalized`], used by the `Vector` and
/// `SparseVector` arms: `Vec<f32>` weights compare via IEEE-754 `==` (so
/// `0.0 == -0.0`), so they must hash with the same normalization to uphold the
/// `Hash`/`Eq` contract.
fn hash_f32_normalized<H: Hasher>(f: f32, state: &mut H) {
    let bits = if f == 0.0 {
        0.0f32.to_bits()
    } else if f.is_nan() {
        f32::NAN.to_bits()
    } else {
        f.to_bits()
    };
    bits.hash(state);
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Entities hash by identity, to agree with `PartialEq` above.
        //
        // The discriminant is deliberately *not* mixed in here: the two
        // encodings of one entity are different variants, and hashing the
        // variant would put equal values in different buckets — a broken
        // `Hash`/`Eq` contract, and silently wrong `HashSet`/`HashMap` results
        // rather than a visible failure.
        if let Some(entity) = self.entity_ref() {
            entity.hash(state);
            return;
        }

        // Discriminant first for type safety
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(i) => i.hash(state),
            // Normalize so that `0.0`/`-0.0` hash alike and all NaNs hash alike,
            // matching `PartialEq` (see `float_eq_normalized`) and upholding the
            // `Hash`/`Eq` contract.
            Value::Float(f) => hash_f64_normalized(*f, state),
            Value::String(s) => s.hash(state),
            Value::Bytes(b) => b.hash(state),
            Value::List(l) => l.hash(state),
            Value::Map(m) => hash_map(m, state),
            Value::Node(n) => n.hash(state),
            Value::Edge(e) => e.hash(state),
            Value::Path(p) => p.hash(state),
            Value::Vector(v) => {
                // `Vec<f32>` compares via IEEE-754 `==` (so `0.0 == -0.0`); hash
                // with the same signed-zero/NaN normalization to stay consistent.
                v.len().hash(state);
                for f in v {
                    hash_f32_normalized(*f, state);
                }
            }
            Value::SparseVector { indices, values } => {
                // Parallel to the `Vector` arm: `Vec<f32>` weights compare via
                // IEEE-754 `==`, so hash with the same signed-zero/NaN
                // normalization to uphold the `Hash`/`Eq` contract.
                indices.hash(state);
                values.len().hash(state);
                for f in values {
                    hash_f32_normalized(*f, state);
                }
            }
            // Exact byte buffer — native `Vec<u8>` hashing, consistent with the
            // native-equality arm above.
            Value::BinaryVector(b) => b.hash(state),
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

    // Float→i64 **truncates toward zero** (`1.9` → `1`). This is deliberate and
    // must not be "fixed" to match the strict `i32` impl below: this conversion
    // backs Cypher's `toInteger()`, whose spec truncates a float. The `i32`
    // impl, by contrast, is the *strict typed* coercion used for schema/storage
    // and rejects out-of-range or fractional floats. The two policies differ on
    // purpose.
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

    // Strict typed coercion (schema/storage): unlike the `i64`/`toInteger`
    // impl above, an out-of-range or fractional float is an error, not a
    // truncation — losing precision when narrowing into a typed column is a
    // bug, not a convenience.
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

/// [`Value::entity_ref`] for a caller that already holds the map.
///
/// Same rules, exposed separately so a hot path (a sort-key encoder, a row
/// decoder) need not clone the map into a [`Value`] just to ask.
#[must_use]
pub fn entity_ref_from_map(map: &HashMap<String, Value>) -> Option<EntityRef> {
    fn id_from(v: &Value) -> Option<u64> {
        match v {
            Value::Int(i) if *i >= 0 => Some(*i as u64),
            Value::String(s) => s
                .strip_prefix("Vid(")
                .or_else(|| s.strip_prefix("Eid("))
                .and_then(|t| t.strip_suffix(')'))
                .unwrap_or(s)
                .parse::<u64>()
                .ok(),
            other => other.as_u64(),
        }
    }

    // An explicit `_eid`/`_vid` settles it on its own; `_id` needs the
    // structural tell, since both encodings spell it that way.
    let looks_like_edge = map.contains_key("_eid")
        || map.contains_key("eid")
        || ((map.contains_key("_src") || map.contains_key("src"))
            && (map.contains_key("_dst") || map.contains_key("dst")))
        || map.contains_key("_type_name")
        || map.contains_key("edge_type");

    if looks_like_edge {
        get_with_fallback(map, &["_eid", "_id", "eid"])
            .and_then(id_from)
            .map(|id| EntityRef::Edge(Eid::from(id)))
    } else {
        get_with_fallback(map, &["_vid", "_id", "vid"])
            .and_then(id_from)
            .map(|id| EntityRef::Vertex(Vid::from(id)))
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
            Value::SparseVector { indices, values } => {
                let idx = serde_json::Value::Array(
                    indices
                        .into_iter()
                        .map(|i| serde_json::Value::Number(serde_json::Number::from(i)))
                        .collect(),
                );
                let vals = serde_json::Value::Array(
                    values
                        .into_iter()
                        .map(|f| {
                            serde_json::Number::from_f64(f as f64)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        })
                        .collect(),
                );
                let mut map = serde_json::Map::new();
                map.insert("indices".to_string(), idx);
                map.insert("values".to_string(), vals);
                serde_json::Value::Object(map)
            }
            // Byte lanes as a JSON array of `0..=255` integers (parallel to the
            // dense `Vector` arm).
            Value::BinaryVector(bytes) => serde_json::Value::Array(
                bytes
                    .into_iter()
                    .map(|b| serde_json::Value::Number(serde_json::Number::from(b)))
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

    mod entity_identity {
        use super::super::{EntityRef, Value};
        use crate::core::id::{Eid, Vid};
        use std::collections::HashMap;

        fn node_map(id: Value) -> Value {
            let mut m = HashMap::new();
            m.insert("_vid".to_string(), id);
            m.insert("_labels".to_string(), Value::List(vec![]));
            Value::Map(m)
        }

        fn edge_map_with_shared_id(id: Value) -> Value {
            // The `_id` spelling both encodings accept, plus edge structure.
            let mut m = HashMap::new();
            m.insert("_id".to_string(), id);
            m.insert("_type".to_string(), Value::String("KNOWS".into()));
            m.insert("_src".to_string(), Value::Int(0));
            m.insert("_dst".to_string(), Value::Int(1));
            Value::Map(m)
        }

        /// Both encodings of one vertex report one identity.
        ///
        /// This is the whole point of the accessor: a site that matched only
        /// `Value::Node` answered "not an entity" for the map form, and the
        /// caller read that as "not equal" rather than as a failure.
        #[test]
        fn the_two_vertex_encodings_agree() {
            let native = Value::Node(crate::value::Node {
                vid: Vid::from(7),
                labels: vec!["P".into()],
                properties: HashMap::new(),
            });
            let as_map = node_map(Value::Int(7));
            assert_eq!(native.entity_ref(), Some(EntityRef::Vertex(Vid::from(7))));
            assert_eq!(as_map.entity_ref(), native.entity_ref());
        }

        /// Both encodings of one edge report one identity.
        #[test]
        fn the_two_edge_encodings_agree() {
            let native = Value::Edge(crate::value::Edge {
                eid: Eid::from(7),
                edge_type: "KNOWS".into(),
                src: Vid::from(0),
                dst: Vid::from(1),
                properties: HashMap::new(),
            });
            assert_eq!(native.entity_ref(), Some(EntityRef::Edge(Eid::from(7))));
            assert_eq!(
                edge_map_with_shared_id(Value::Int(7)).entity_ref(),
                native.entity_ref()
            );
        }

        /// An edge is not the vertex of the same number.
        ///
        /// `_id` is a spelling both encodings use, so without the structural
        /// tell an edge map resolved to `Vertex(7)` — two different entities
        /// reporting one identity, which is a wrong answer in whichever
        /// direction the comparison then went.
        #[test]
        fn an_edge_is_not_the_vertex_of_the_same_number() {
            let edge = edge_map_with_shared_id(Value::Int(7));
            let vertex = node_map(Value::Int(7));
            assert_eq!(edge.entity_ref(), Some(EntityRef::Edge(Eid::from(7))));
            assert_eq!(vertex.entity_ref(), Some(EntityRef::Vertex(Vid::from(7))));
            assert_ne!(edge.entity_ref(), vertex.entity_ref());
            // And the narrow accessors do not cross over.
            assert_eq!(edge.entity_vid(), None);
            assert_eq!(vertex.entity_eid(), None);
        }

        /// The serde spelling `"Vid(7)"` resolves, and so does its edge twin.
        #[test]
        fn the_debug_rendered_id_spellings_resolve() {
            assert_eq!(
                node_map(Value::String("Vid(7)".into())).entity_vid(),
                Some(Vid::from(7))
            );
            assert_eq!(
                edge_map_with_shared_id(Value::String("Eid(7)".into())).entity_eid(),
                Some(Eid::from(7))
            );
            // A plain numeric string is still accepted.
            assert_eq!(
                node_map(Value::String("7".into())).entity_vid(),
                Some(Vid::from(7))
            );
        }

        /// A bare integer is not an entity, and a negative id is not one either.
        #[test]
        fn non_entities_stay_non_entities() {
            assert_eq!(Value::Int(7).entity_ref(), None);
            assert_eq!(Value::String("7".into()).entity_ref(), None);
            assert_eq!(Value::Null.entity_ref(), None);
            assert_eq!(node_map(Value::Int(-1)).entity_ref(), None);
        }

        fn hash_of(v: &Value) -> u64 {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            v.hash(&mut h);
            h.finish()
        }

        /// `Value`'s own `==` compares entities by identity, so a site that
        /// never heard of `entity_ref` still gets the right answer.
        ///
        /// This is the boundary fix: `HashSet<Value>` backs `count(DISTINCT …)`
        /// and the recursive-CTE cycle-detection set, and Locy's `values_equal`
        /// falls through to here, so this operator reaches user-visible results.
        #[test]
        fn value_equality_compares_entities_by_identity() {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("a".into()));
            let hydrated = Value::Node(crate::value::Node {
                vid: Vid::from(7),
                labels: vec!["P".into(), "Q".into()],
                properties: props,
            });
            let bare = Value::Node(crate::value::Node {
                vid: Vid::from(7),
                labels: vec!["P".into()],
                properties: HashMap::new(),
            });
            // Same vertex, different hydration.
            assert_eq!(hydrated, bare);
            // Same vertex, different encoding.
            assert_eq!(hydrated, node_map(Value::Int(7)));
            // Different vertices stay different.
            assert_ne!(hydrated, node_map(Value::Int(8)));
            // An entity is not a bare number, and not an edge of that number.
            assert_ne!(hydrated, Value::Int(7));
            assert_ne!(hydrated, edge_map_with_shared_id(Value::Int(7)));
        }

        /// `Hash` agrees with `Eq`, including across the two encodings.
        ///
        /// If these disagreed, equal values would land in different buckets and
        /// every `HashSet`/`HashMap` keyed on `Value` would silently keep
        /// duplicates — a wrong answer rather than a visible failure. The
        /// discriminant is therefore not hashed for entities, since the two
        /// encodings are different variants.
        #[test]
        fn value_hash_agrees_with_equality_across_encodings() {
            let native = Value::Node(crate::value::Node {
                vid: Vid::from(7),
                labels: vec!["P".into()],
                properties: HashMap::new(),
            });
            let as_map = node_map(Value::Int(7));
            assert_eq!(native, as_map);
            assert_eq!(
                hash_of(&native),
                hash_of(&as_map),
                "equal values must hash alike or HashSet keeps both"
            );
        }

        /// The dedup a `HashSet<Value>` performs is now identity-based.
        #[test]
        fn a_value_set_dedups_one_entity_across_encodings() {
            use std::collections::HashSet;
            let mut set: HashSet<Value> = HashSet::new();
            set.insert(Value::Node(crate::value::Node {
                vid: Vid::from(7),
                labels: vec!["P".into()],
                properties: HashMap::new(),
            }));
            set.insert(node_map(Value::Int(7)));
            set.insert(edge_map_with_shared_id(Value::Int(7)));
            set.insert(node_map(Value::Int(8)));
            assert_eq!(
                set.len(),
                3,
                "one vertex twice-encoded is one member; the edge and the other vertex are two more"
            );
        }

        /// `EntityRef` is usable as a dedup/join key, which is what lets one
        /// type serve equality, DISTINCT, joins and IN alike.
        #[test]
        fn entity_ref_is_a_usable_key() {
            use std::collections::HashSet;
            let mut seen = HashSet::new();
            assert!(
                seen.insert(
                    Value::Node(crate::value::Node {
                        vid: Vid::from(7),
                        labels: vec!["A".into()],
                        properties: HashMap::new(),
                    })
                    .entity_ref()
                )
            );
            // The same vertex in the other encoding, with different properties
            // and labels, must not count twice.
            assert!(!seen.insert(node_map(Value::Int(7)).entity_ref()));
            // A different vertex must.
            assert!(seen.insert(node_map(Value::Int(8)).entity_ref()));
            // An edge of the same number is a distinct entity.
            assert!(seen.insert(edge_map_with_shared_id(Value::Int(7)).entity_ref()));
        }
    }

    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn cmp_i64_f64_exact_above_2p53() {
        // 2^53 vs 2^53+1: the naive `as f64` cast collapses these to Equal.
        let two_p53 = 9_007_199_254_740_992.0_f64;
        assert_eq!(cmp_i64_f64(9_007_199_254_740_992, two_p53), Ordering::Equal);
        assert_eq!(
            cmp_i64_f64(9_007_199_254_740_993, two_p53),
            Ordering::Greater
        );
        assert_eq!(cmp_i64_f64(9_007_199_254_740_991, two_p53), Ordering::Less);
    }

    #[test]
    fn cmp_i64_f64_small_and_fractional() {
        assert_eq!(cmp_i64_f64(2, 2.0), Ordering::Equal);
        assert_eq!(cmp_i64_f64(1, 1.5), Ordering::Less);
        assert_eq!(cmp_i64_f64(2, 1.5), Ordering::Greater);
        assert_eq!(cmp_i64_f64(-3, -2.5), Ordering::Less);
        assert_eq!(cmp_i64_f64(-2, -2.5), Ordering::Greater);
        assert_eq!(cmp_i64_f64(0, -0.0), Ordering::Equal);
    }

    #[test]
    fn cmp_i64_f64_extremes_and_infinities() {
        assert_eq!(cmp_i64_f64(i64::MAX, f64::INFINITY), Ordering::Less);
        assert_eq!(cmp_i64_f64(i64::MIN, f64::NEG_INFINITY), Ordering::Greater);
        // i64::MAX = 2^63 - 1; the f64 2^63 is strictly larger.
        assert_eq!(
            cmp_i64_f64(i64::MAX, 9_223_372_036_854_775_808.0),
            Ordering::Less
        );
        // i64::MIN = -2^63, exactly representable as f64 -> Equal.
        assert_eq!(
            cmp_i64_f64(i64::MIN, -9_223_372_036_854_775_808.0),
            Ordering::Equal
        );
        // A float below -2^63 is smaller than any i64.
        assert_eq!(cmp_i64_f64(i64::MIN, -1e300), Ordering::Greater);
        // A huge positive float dwarfs any i64.
        assert_eq!(cmp_i64_f64(i64::MAX, 1e300), Ordering::Less);
    }

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

    /// Regression: `Value` `Hash`/`Eq` contract violation on signed-zero floats.
    ///
    /// `Value::Float` compares via IEEE-754 (`0.0 == -0.0`) but hashes via
    /// `f64::to_bits`, where `0.0` and `-0.0` differ. The std contract requires
    /// `k1 == k2` to imply `hash(k1) == hash(k2)`; violating it corrupts
    /// `HashMap<Vec<Value>, _>` keys used for `PARTITION BY`.
    #[test]
    fn value_hash_eq_contract_float_signed_zero() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn h(v: &Value) -> u64 {
            let mut s = DefaultHasher::new();
            v.hash(&mut s);
            s.finish()
        }

        let pos = Value::Float(0.0);
        let neg = Value::Float(-0.0);
        assert_eq!(pos, neg, "0.0 and -0.0 compare equal");
        assert_eq!(
            h(&pos),
            h(&neg),
            "equal Values must hash equally (Hash/Eq contract)"
        );
    }
}
