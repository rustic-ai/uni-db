// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Temporal functions for Cypher query evaluation.
//!
//! Provides date, time, datetime, and duration constructors along with
//! extraction functions compatible with OpenCypher temporal types.

use anyhow::{Result, anyhow};
use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset,
    TimeZone, Timelike, Utc,
};
use chrono_tz::Tz;
use std::collections::HashMap;
use uni_common::Value;

// ============================================================================
// Constants
// ============================================================================

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
const SECONDS_PER_DAY: i64 = 86_400;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 24 * 3600 * NANOS_PER_SECOND;

// ============================================================================
// Temporal Type Classification
// ============================================================================

/// Classification of temporal string values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalType {
    /// Date only: "1984-10-11"
    Date,
    /// Local time (no timezone): "12:31:14" / "12:31:14.645876123"
    LocalTime,
    /// Time with timezone: "12:31:14+01:00" / "12:31:14Z"
    Time,
    /// Local datetime (no timezone): "1984-10-11T12:31:14"
    LocalDateTime,
    /// Datetime with timezone: "1984-10-11T12:31:14+01:00" / "...Z"
    DateTime,
    /// ISO 8601 duration: "P1Y2M3DT4H5M6S"
    Duration,
}

/// Classify a string value into its temporal type using pattern detection.
pub fn classify_temporal(s: &str) -> Option<TemporalType> {
    // Strip bracketed timezone suffix for classification
    let base = if let Some(bracket_pos) = s.find('[') {
        &s[..bracket_pos]
    } else {
        s
    };

    // Duration: starts with P (case insensitive)
    if base.starts_with('P') || base.starts_with('p') {
        return Some(TemporalType::Duration);
    }

    // Check for date component (YYYY-MM-DD pattern)
    let has_date = base.len() >= 10
        && base.as_bytes().get(4) == Some(&b'-')
        && base.as_bytes().get(7) == Some(&b'-')
        && base[..4].bytes().all(|b| b.is_ascii_digit())
        && base[5..7].bytes().all(|b| b.is_ascii_digit())
        && base[8..10].bytes().all(|b| b.is_ascii_digit());

    // Check for T separator indicating datetime
    let has_t = has_date && base.len() > 10 && base.as_bytes().get(10) == Some(&b'T');

    if has_date && has_t {
        // Has both date and time components
        let after_t = &base[11..];
        if has_timezone_suffix(after_t) {
            Some(TemporalType::DateTime)
        } else {
            Some(TemporalType::LocalDateTime)
        }
    } else if has_date {
        Some(TemporalType::Date)
    } else {
        // Try time patterns: HH:MM:SS or HH:MM:SS.fff
        let has_time = base.len() >= 5
            && base.as_bytes().get(2) == Some(&b':')
            && base[..2].bytes().all(|b| b.is_ascii_digit())
            && base[3..5].bytes().all(|b| b.is_ascii_digit());

        if has_time {
            if has_timezone_suffix(base) {
                Some(TemporalType::Time)
            } else {
                Some(TemporalType::LocalTime)
            }
        } else {
            None
        }
    }
}

/// Check if a temporal string suffix contains timezone information.
fn has_timezone_suffix(s: &str) -> bool {
    if s.ends_with('Z') || s.ends_with('z') {
        return true;
    }
    // Look for +HH:MM or -HH:MM at the end, accounting for possible [timezone]
    let check = s;
    // Find last occurrence of + or - that could be a timezone offset
    // Must be after the time digits (at least HH:MM:SS = 8 chars, or with fractional seconds)
    for (i, b) in check.bytes().enumerate().rev() {
        if b == b'+' || b == b'-' {
            // Check if the position makes sense as timezone offset (not negative number)
            let after = &check[i + 1..];
            if after.len() >= 4
                && after[..2].bytes().all(|b| b.is_ascii_digit())
                && after.as_bytes().get(2) == Some(&b':')
            {
                return true;
            }
            // Could be +HHMM format
            if after.len() >= 4 && after[..4].bytes().all(|b| b.is_ascii_digit()) {
                return true;
            }
        }
    }
    false
}

/// Check if a string value matches a specific temporal type.
fn is_temporal_type(val: &Value, expected: TemporalType) -> bool {
    matches!(val, Value::String(s) if classify_temporal(s) == Some(expected))
}

/// Check if a string value is a local time (no timezone).
pub fn is_localtime_value(val: &Value) -> bool {
    is_temporal_type(val, TemporalType::LocalTime)
}

/// Check if a string value is a time with timezone.
pub fn is_time_value(val: &Value) -> bool {
    is_temporal_type(val, TemporalType::Time)
}

/// Check if a string value is a local datetime (no timezone).
pub fn is_localdatetime_value(val: &Value) -> bool {
    is_temporal_type(val, TemporalType::LocalDateTime)
}

/// Parse a duration from a Value, handling both ISO 8601 strings and integer microseconds.
pub fn parse_duration_from_value(val: &Value) -> Result<CypherDuration> {
    match val {
        Value::String(s) => parse_duration_to_cypher(s),
        Value::Int(micros) => Ok(CypherDuration::from_micros(*micros)),
        _ => Err(anyhow!("Expected duration value")),
    }
}

// ============================================================================
// Timezone Handling
// ============================================================================

/// Parsed timezone information.
#[derive(Debug, Clone)]
pub enum TimezoneInfo {
    /// Fixed offset timezone (e.g., +01:00, -05:00, Z)
    FixedOffset(FixedOffset),
    /// Named IANA timezone (e.g., Europe/Stockholm)
    Named(Tz),
}

impl TimezoneInfo {
    /// Get the offset in seconds for a given local datetime.
    pub fn offset_for_local(&self, ndt: &NaiveDateTime) -> Result<FixedOffset> {
        match self {
            TimezoneInfo::FixedOffset(fo) => Ok(*fo),
            TimezoneInfo::Named(tz) => {
                // Get the offset for the given local time
                match tz.from_local_datetime(ndt) {
                    chrono::LocalResult::Single(dt) => Ok(dt.offset().fix()),
                    chrono::LocalResult::Ambiguous(dt1, _dt2) => {
                        // During DST transition, pick the earlier one (standard time)
                        Ok(dt1.offset().fix())
                    }
                    chrono::LocalResult::None => {
                        // Time doesn't exist (DST gap), find the closest valid time
                        Err(anyhow!("Local time does not exist in timezone (DST gap)"))
                    }
                }
            }
        }
    }

    /// Get the timezone name for output formatting.
    fn name(&self) -> Option<&str> {
        match self {
            TimezoneInfo::FixedOffset(_) => None,
            TimezoneInfo::Named(tz) => Some(tz.name()),
        }
    }

    /// Get offset seconds for a fixed offset timezone, or for a named timezone at a given date.
    fn offset_seconds_with_date(&self, date: &NaiveDate) -> i32 {
        match self {
            TimezoneInfo::FixedOffset(fo) => fo.local_minus_utc(),
            TimezoneInfo::Named(tz) => {
                // Use noon on the date to calculate offset (avoids DST transition edge cases)
                let noon = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
                let ndt = NaiveDateTime::new(*date, noon);
                match tz.from_local_datetime(&ndt) {
                    chrono::LocalResult::Single(dt) => dt.offset().fix().local_minus_utc(),
                    chrono::LocalResult::Ambiguous(dt1, _) => dt1.offset().fix().local_minus_utc(),
                    chrono::LocalResult::None => 0, // Fallback, shouldn't happen at noon
                }
            }
        }
    }
}

/// Parse timezone - supports fixed offsets (+01:00) and IANA names (Europe/Stockholm).
fn parse_timezone(tz_str: &str) -> Result<TimezoneInfo> {
    let tz_str = tz_str.trim();

    // Try parsing as IANA timezone name first
    if let Ok(tz) = tz_str.parse::<Tz>() {
        return Ok(TimezoneInfo::Named(tz));
    }

    // Try parsing as fixed offset
    let offset_secs = parse_timezone_offset(tz_str)?;
    let offset = FixedOffset::east_opt(offset_secs)
        .ok_or_else(|| anyhow!("Invalid timezone offset: {}", offset_secs))?;
    Ok(TimezoneInfo::FixedOffset(offset))
}

// ============================================================================
// Public API
// ============================================================================

/// Parse a datetime string into a `DateTime<Utc>`.
///
/// Supports multiple formats:
/// - RFC3339 (e.g., "2023-01-01T00:00:00Z")
/// - "%Y-%m-%d %H:%M:%S %z" (e.g., "2023-01-01 00:00:00 +0000")
/// - "%Y-%m-%d %H:%M:%S" naive (assumed UTC)
///
/// This is the canonical datetime parsing function for temporal operations
/// like `validAt`. Using a single implementation ensures consistent behavior.
pub fn parse_datetime_utc(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
        .or_else(|_| {
            // Handle formats without seconds (e.g., "2023-01-01T00:00Z")
            if let Some(base) = s.strip_suffix('Z') {
                NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M")
                    .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
            } else {
                // Handle formats without seconds with offset (e.g., "2023-01-01T00:00+05:00")
                DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M%:z")
                    .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
            }
        })
        .or_else(|_| {
            DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %z")
                .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
        })
        .map_err(|_| anyhow!("Invalid datetime format: {}", s))
}

/// Evaluate date/time functions.
///
/// Routes to the appropriate handler based on function name. Supports:
/// - Basic constructors: DATE, TIME, DATETIME, LOCALDATETIME, LOCALTIME, DURATION
/// - Extraction: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
/// - Dotted namespace functions: DATETIME.FROMEPOCH, DATE.TRUNCATE, etc.
pub fn eval_datetime_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        // Basic constructors
        "DATE" => eval_date(args),
        "TIME" => eval_time(args),
        "DATETIME" => eval_datetime(args),
        "LOCALDATETIME" => eval_localdatetime(args),
        "LOCALTIME" => eval_localtime(args),
        "DURATION" => eval_duration(args),

        // Extraction functions
        "YEAR" => eval_extract(args, Component::Year),
        "MONTH" => eval_extract(args, Component::Month),
        "DAY" => eval_extract(args, Component::Day),
        "HOUR" => eval_extract(args, Component::Hour),
        "MINUTE" => eval_extract(args, Component::Minute),
        "SECOND" => eval_extract(args, Component::Second),

        // Epoch functions
        "DATETIME.FROMEPOCH" => eval_datetime_fromepoch(args),
        "DATETIME.FROMEPOCHMILLIS" => eval_datetime_fromepochmillis(args),

        // Truncate functions
        "DATE.TRUNCATE" => eval_truncate("date", args),
        "TIME.TRUNCATE" => eval_truncate("time", args),
        "DATETIME.TRUNCATE" => eval_truncate("datetime", args),
        "LOCALDATETIME.TRUNCATE" => eval_truncate("localdatetime", args),
        "LOCALTIME.TRUNCATE" => eval_truncate("localtime", args),

        // Transaction/statement/realtime functions (return current time)
        "DATETIME.TRANSACTION" | "DATETIME.STATEMENT" | "DATETIME.REALTIME" => eval_datetime(args),
        "DATE.TRANSACTION" | "DATE.STATEMENT" | "DATE.REALTIME" => eval_date(args),
        "TIME.TRANSACTION" | "TIME.STATEMENT" | "TIME.REALTIME" => eval_time(args),
        "LOCALTIME.TRANSACTION" | "LOCALTIME.STATEMENT" | "LOCALTIME.REALTIME" => {
            eval_localtime(args)
        }
        "LOCALDATETIME.TRANSACTION" | "LOCALDATETIME.STATEMENT" | "LOCALDATETIME.REALTIME" => {
            eval_localdatetime(args)
        }

        // Duration between functions
        "DURATION.BETWEEN" => eval_duration_between(args),
        "DURATION.INMONTHS" => eval_duration_in_months(args),
        "DURATION.INDAYS" => eval_duration_in_days(args),
        "DURATION.INSECONDS" => eval_duration_in_seconds(args),

        _ => Err(anyhow!("Unknown datetime function: {}", name)),
    }
}

/// Check if value is a datetime string.
pub fn is_datetime_value(val: &Value) -> bool {
    match val {
        Value::String(s) => parse_datetime_utc(s).is_ok(),
        _ => false,
    }
}

/// Check if value is a date string.
pub fn is_date_value(val: &Value) -> bool {
    match val {
        Value::String(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok(),
        _ => false,
    }
}

/// Check if value is a duration (ISO 8601 string starting with 'P').
///
/// Note: Numbers are NOT automatically treated as durations. The duration()
/// function can accept numbers as microseconds, but arbitrary numbers in
/// arithmetic expressions should not be interpreted as durations.
pub fn is_duration_value(val: &Value) -> bool {
    match val {
        Value::String(s) => s.starts_with('P') || s.starts_with('p'),
        _ => false,
    }
}

/// Check if a value is a duration string OR an integer (microseconds).
///
/// This is used for temporal arithmetic where integers are implicitly treated
/// as durations when paired with datetime/date values. For standalone type
/// checking, use `is_duration_value` instead.
pub fn is_duration_or_micros(val: &Value) -> bool {
    is_duration_value(val) || matches!(val, Value::Int(_))
}

/// Convert a duration value (ISO 8601 string or i64 micros) to microseconds.
pub fn duration_to_micros(val: &Value) -> Result<i64> {
    match val {
        Value::String(s) => {
            let duration = parse_duration_to_cypher(s)?;
            Ok(duration.to_micros())
        }
        Value::Int(i) => Ok(*i),
        _ => Err(anyhow!("Expected duration value")),
    }
}

/// Add duration (microseconds) to datetime.
pub fn add_duration_to_datetime(dt_str: &str, micros: i64) -> Result<String> {
    let dt = parse_datetime_utc(dt_str)?;
    let result = dt + Duration::microseconds(micros);
    Ok(result.to_rfc3339())
}

/// Add duration (microseconds) to date.
pub fn add_duration_to_date(date_str: &str, micros: i64) -> Result<String> {
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?;
    let dt = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow!("Invalid date"))?;
    let result = dt + Duration::microseconds(micros);
    Ok(result.format("%Y-%m-%d").to_string())
}

/// Subtract two datetimes, return duration in microseconds.
pub fn datetime_difference(dt1_str: &str, dt2_str: &str) -> Result<i64> {
    let dt1 = parse_datetime_utc(dt1_str)?;
    let dt2 = parse_datetime_utc(dt2_str)?;
    dt1.signed_duration_since(dt2)
        .num_microseconds()
        .ok_or_else(|| anyhow!("Duration overflow"))
}

/// Parse a duration string to microseconds.
///
/// Supports ISO 8601 format (P1DT1H30M) and simple formats (1h30m, 90s, etc.)
pub fn parse_duration_to_micros(s: &str) -> Result<i64> {
    let s = s.trim();

    // ISO 8601 format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
    if s.starts_with('P') || s.starts_with('p') {
        return parse_iso8601_duration(s);
    }

    // Simple format: combinations of NdNhNmNs (e.g., "1d2h30m", "90s", "1h30m")
    parse_simple_duration(s)
}

/// Parse a duration string to a CypherDuration with preserved components.
pub fn parse_duration_to_cypher(s: &str) -> Result<CypherDuration> {
    let s = s.trim();

    // ISO 8601 format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
    if s.starts_with('P') || s.starts_with('p') {
        return parse_iso8601_duration_cypher(s);
    }

    // Simple format: fall back to microseconds conversion
    let micros = parse_simple_duration(s)?;
    Ok(CypherDuration::from_micros(micros))
}

/// Parse ISO 8601 duration format to CypherDuration (preserves month/day/time components).
fn parse_iso8601_duration_cypher(s: &str) -> Result<CypherDuration> {
    let s = &s[1..]; // Skip 'P'
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut nanos: i64 = 0;
    let mut in_time_part = false;
    let mut num_buf = String::new();

    for c in s.chars() {
        if c == 'T' || c == 't' {
            in_time_part = true;
            continue;
        }

        if c.is_ascii_digit() || c == '.' || c == '-' {
            num_buf.push(c);
        } else {
            if num_buf.is_empty() {
                continue;
            }
            let num: f64 = num_buf
                .parse()
                .map_err(|_| anyhow!("Invalid duration number"))?;
            num_buf.clear();

            match c {
                'Y' | 'y' => months += (num * 12.0) as i64,
                'M' if !in_time_part => months += num as i64,
                'W' | 'w' => days += (num * 7.0) as i64,
                'D' | 'd' => days += num as i64,
                'H' | 'h' => nanos += (num * 3600.0 * NANOS_PER_SECOND as f64) as i64,
                'M' | 'm' if in_time_part => nanos += (num * 60.0 * NANOS_PER_SECOND as f64) as i64,
                'S' | 's' => nanos += (num * NANOS_PER_SECOND as f64) as i64,
                _ => return Err(anyhow!("Invalid ISO 8601 duration designator: {}", c)),
            }
        }
    }

    Ok(CypherDuration::new(months, days, nanos))
}

// ============================================================================
// Component Extraction
// ============================================================================

enum Component {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

fn eval_extract(args: &[Value], component: Component) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("Extract function requires 1 argument"));
    }
    match &args[0] {
        Value::String(s) => {
            // Try parsing as DateTime, then NaiveDateTime, then NaiveDate, then NaiveTime
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                return Ok(Value::Int(extract_component(&dt, &component) as i64));
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                return Ok(Value::Int(extract_component(&dt, &component) as i64));
            }

            match component {
                Component::Year | Component::Month | Component::Day => {
                    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        return Ok(Value::Int(match component {
                            Component::Year => d.year() as i64,
                            Component::Month => d.month() as i64,
                            Component::Day => d.day() as i64,
                            _ => unreachable!(),
                        }));
                    }
                }
                Component::Hour | Component::Minute | Component::Second => {
                    if let Ok(t) = NaiveTime::parse_from_str(s, "%H:%M:%S") {
                        return Ok(Value::Int(match component {
                            Component::Hour => t.hour() as i64,
                            Component::Minute => t.minute() as i64,
                            Component::Second => t.second() as i64,
                            _ => unreachable!(),
                        }));
                    }
                }
            }

            Err(anyhow!("Could not parse date/time string for extraction"))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("Extract function expects a string argument")),
    }
}

fn extract_component<T: Datelike + Timelike>(dt: &T, component: &Component) -> i32 {
    match component {
        Component::Year => dt.year(),
        Component::Month => dt.month() as i32,
        Component::Day => dt.day() as i32,
        Component::Hour => dt.hour() as i32,
        Component::Minute => dt.minute() as i32,
        Component::Second => dt.second() as i32,
    }
}

// ============================================================================
// Temporal Component Accessors (for property access on temporals)
// ============================================================================

/// Evaluate a temporal component accessor.
///
/// This handles property access on temporal values like `dt.quarter`, `dt.week`,
/// `dt.dayOfWeek`, `dt.timezone`, etc.
pub fn eval_temporal_accessor(temporal_str: &str, component: &str) -> Result<Value> {
    let component_lower = component.to_lowercase();
    match component_lower.as_str() {
        // Basic date components (already handled by eval_extract but also here for consistency)
        "year" => extract_year(temporal_str),
        "month" => extract_month(temporal_str),
        "day" => extract_day(temporal_str),
        "hour" => extract_hour(temporal_str),
        "minute" => extract_minute(temporal_str),
        "second" => extract_second(temporal_str),

        // Extended date components
        "quarter" => extract_quarter(temporal_str),
        "week" => extract_week(temporal_str),
        "weekyear" => extract_week_year(temporal_str),
        "ordinalday" => extract_ordinal_day(temporal_str),
        "dayofweek" | "weekday" => extract_day_of_week(temporal_str),
        "dayofquarter" => extract_day_of_quarter(temporal_str),

        // Sub-second components
        "millisecond" => extract_millisecond(temporal_str),
        "microsecond" => extract_microsecond(temporal_str),
        "nanosecond" => extract_nanosecond(temporal_str),

        // Timezone components
        "timezone" => extract_timezone_name_from_str(temporal_str),
        "offset" => extract_offset_string(temporal_str),
        "offsetminutes" => extract_offset_minutes(temporal_str),
        "offsetseconds" => extract_offset_seconds(temporal_str),

        // Epoch components
        "epochseconds" => extract_epoch_seconds(temporal_str),
        "epochmillis" => extract_epoch_millis(temporal_str),

        _ => Err(anyhow!("Unknown temporal component: {}", component)),
    }
}

/// Check if a property name is a valid temporal accessor.
pub fn is_temporal_accessor(property: &str) -> bool {
    let property_lower = property.to_lowercase();
    matches!(
        property_lower.as_str(),
        "year"
            | "month"
            | "day"
            | "hour"
            | "minute"
            | "second"
            | "quarter"
            | "week"
            | "weekyear"
            | "ordinalday"
            | "dayofweek"
            | "weekday"
            | "dayofquarter"
            | "millisecond"
            | "microsecond"
            | "nanosecond"
            | "timezone"
            | "offset"
            | "offsetminutes"
            | "offsetseconds"
            | "epochseconds"
            | "epochmillis"
    )
}

/// Check if a string looks like a temporal value (date, time, datetime).
pub fn is_temporal_string(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 8 {
        return false;
    }

    // Date pattern: YYYY-MM-DD
    (bytes.len() >= 10 && bytes[4] == b'-' && bytes[7] == b'-')
    // Time pattern: HH:MM:SS
    || (bytes[2] == b':' && bytes[5] == b':')
    // Duration pattern: starts with P
    || bytes[0] == b'P' || bytes[0] == b'p'
}

/// Check if a string looks like a duration value.
pub fn is_duration_string(s: &str) -> bool {
    s.starts_with('P') || s.starts_with('p')
}

// Individual component extractors

fn extract_date_component(s: &str, f: impl FnOnce(NaiveDate) -> i64) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    Ok(Value::Int(f(date)))
}

fn extract_time_component(s: &str, f: impl FnOnce(NaiveTime) -> i64) -> Result<Value> {
    let (_, time, _) = parse_datetime_with_tz(s)?;
    Ok(Value::Int(f(time)))
}

fn extract_year(s: &str) -> Result<Value> {
    extract_date_component(s, |d| d.year() as i64)
}

fn extract_month(s: &str) -> Result<Value> {
    extract_date_component(s, |d| d.month() as i64)
}

fn extract_day(s: &str) -> Result<Value> {
    extract_date_component(s, |d| d.day() as i64)
}

fn extract_hour(s: &str) -> Result<Value> {
    extract_time_component(s, |t| t.hour() as i64)
}

fn extract_minute(s: &str) -> Result<Value> {
    extract_time_component(s, |t| t.minute() as i64)
}

fn extract_second(s: &str) -> Result<Value> {
    extract_time_component(s, |t| t.second() as i64)
}

fn extract_quarter(s: &str) -> Result<Value> {
    extract_date_component(s, |d| ((d.month() - 1) / 3 + 1) as i64)
}

fn extract_week(s: &str) -> Result<Value> {
    extract_date_component(s, |d| d.iso_week().week() as i64)
}

fn extract_week_year(s: &str) -> Result<Value> {
    extract_date_component(s, |d| d.iso_week().year() as i64)
}

fn extract_ordinal_day(s: &str) -> Result<Value> {
    extract_date_component(s, |d| d.ordinal() as i64)
}

fn extract_day_of_week(s: &str) -> Result<Value> {
    // ISO weekday: Monday = 1, Sunday = 7
    extract_date_component(s, |d| (d.weekday().num_days_from_monday() + 1) as i64)
}

fn extract_day_of_quarter(s: &str) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    let quarter = (date.month() - 1) / 3;
    let first_month_of_quarter = quarter * 3 + 1;
    let quarter_start = NaiveDate::from_ymd_opt(date.year(), first_month_of_quarter, 1)
        .ok_or_else(|| {
            anyhow!(
                "Invalid quarter start for year={}, month={}",
                date.year(),
                first_month_of_quarter
            )
        })?;
    let day_of_quarter = (date - quarter_start).num_days() + 1;
    Ok(Value::Int(day_of_quarter))
}

fn extract_millisecond(s: &str) -> Result<Value> {
    extract_time_component(s, |t| (t.nanosecond() / 1_000_000) as i64)
}

fn extract_microsecond(s: &str) -> Result<Value> {
    extract_time_component(s, |t| (t.nanosecond() / 1_000) as i64)
}

fn extract_nanosecond(s: &str) -> Result<Value> {
    extract_time_component(s, |t| t.nanosecond() as i64)
}

fn extract_timezone_name_from_str(s: &str) -> Result<Value> {
    let (_, _, tz_info) = parse_datetime_with_tz(s)?;
    match tz_info {
        Some(TimezoneInfo::Named(tz)) => Ok(Value::String(tz.name().to_string())),
        Some(TimezoneInfo::FixedOffset(offset)) => {
            // Format as offset string with optional seconds
            let secs = offset.local_minus_utc();
            Ok(Value::String(format_timezone_offset(secs)))
        }
        None => Ok(Value::Null),
    }
}

fn extract_offset_string(s: &str) -> Result<Value> {
    let (date, time, tz_info) = parse_datetime_with_tz(s)?;
    match tz_info {
        Some(ref tz) => {
            let ndt = NaiveDateTime::new(date, time);
            let offset = tz.offset_for_local(&ndt)?;
            Ok(Value::String(format_timezone_offset(
                offset.local_minus_utc(),
            )))
        }
        None => Ok(Value::Null),
    }
}

fn extract_offset_total_seconds(s: &str) -> Result<i32> {
    let (date, time, tz_info) = parse_datetime_with_tz(s)?;
    match tz_info {
        Some(ref tz) => {
            let ndt = NaiveDateTime::new(date, time);
            let offset = tz.offset_for_local(&ndt)?;
            Ok(offset.local_minus_utc())
        }
        None => Ok(0),
    }
}

fn extract_offset_minutes(s: &str) -> Result<Value> {
    Ok(Value::Int((extract_offset_total_seconds(s)? / 60) as i64))
}

fn extract_offset_seconds(s: &str) -> Result<Value> {
    Ok(Value::Int(extract_offset_total_seconds(s)? as i64))
}

fn parse_as_utc(s: &str) -> Result<DateTime<Utc>> {
    let (date, time, _) = parse_datetime_with_tz(s)?;
    let ndt = NaiveDateTime::new(date, time);
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
}

fn extract_epoch_seconds(s: &str) -> Result<Value> {
    Ok(Value::Int(parse_as_utc(s)?.timestamp()))
}

fn extract_epoch_millis(s: &str) -> Result<Value> {
    Ok(Value::Int(parse_as_utc(s)?.timestamp_millis()))
}

// ============================================================================
// Duration Component Accessors
// ============================================================================

/// Evaluate a duration component accessor using Euclidean division.
///
/// Uses `div_euclid()` / `rem_euclid()` so remainders are always non-negative,
/// matching the TCK expectations for negative durations.
pub fn eval_duration_accessor(duration_str: &str, component: &str) -> Result<Value> {
    let duration = parse_duration_to_cypher(duration_str)?;
    let component_lower = component.to_lowercase();

    let total_months = duration.months;
    let total_nanos = duration.nanos;
    let total_secs = total_nanos.div_euclid(NANOS_PER_SECOND);

    match component_lower.as_str() {
        // Total components (converted to that unit)
        "years" => Ok(Value::Int(total_months.div_euclid(12))),
        "months" => Ok(Value::Int(total_months)),
        "weeks" => Ok(Value::Int(duration.days.div_euclid(7))),
        "days" => Ok(Value::Int(duration.days)),
        "hours" => Ok(Value::Int(total_secs.div_euclid(3600))),
        "minutes" => Ok(Value::Int(total_secs.div_euclid(60))),
        "seconds" => Ok(Value::Int(total_secs)),
        "milliseconds" => Ok(Value::Int(total_nanos.div_euclid(1_000_000))),
        "microseconds" => Ok(Value::Int(total_nanos.div_euclid(1_000))),
        "nanoseconds" => Ok(Value::Int(total_nanos)),

        // "Of" accessors (remainder within larger unit) using Euclidean remainder
        "quartersofyear" => Ok(Value::Int(total_months.rem_euclid(12) / 3)),
        "monthsofquarter" => Ok(Value::Int(total_months.rem_euclid(3))),
        "monthsofyear" => Ok(Value::Int(total_months.rem_euclid(12))),
        "daysofweek" => Ok(Value::Int(duration.days.rem_euclid(7))),
        "hoursofday" => Ok(Value::Int(total_secs.div_euclid(3600).rem_euclid(24))),
        "minutesofhour" => Ok(Value::Int(total_secs.div_euclid(60).rem_euclid(60))),
        "secondsofminute" => Ok(Value::Int(total_secs.rem_euclid(60))),
        "millisecondsofsecond" => Ok(Value::Int(
            total_nanos.div_euclid(1_000_000).rem_euclid(1000),
        )),
        "microsecondsofsecond" => Ok(Value::Int(
            total_nanos.div_euclid(1_000).rem_euclid(1_000_000),
        )),
        "nanosecondsofsecond" => Ok(Value::Int(total_nanos.rem_euclid(NANOS_PER_SECOND))),

        _ => Err(anyhow!("Unknown duration component: {}", component)),
    }
}

/// Check if a property name is a valid duration accessor.
pub fn is_duration_accessor(property: &str) -> bool {
    let property_lower = property.to_lowercase();
    matches!(
        property_lower.as_str(),
        "years"
            | "months"
            | "weeks"
            | "days"
            | "hours"
            | "minutes"
            | "seconds"
            | "milliseconds"
            | "microseconds"
            | "nanoseconds"
            | "quartersofyear"
            | "monthsofquarter"
            | "monthsofyear"
            | "daysofweek"
            | "hoursofday"
            | "minutesofhour"
            | "secondsofminute"
            | "millisecondsofsecond"
            | "microsecondsofsecond"
            | "nanosecondsofsecond"
    )
}

// ============================================================================
// Date Constructor
// ============================================================================

fn eval_date(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        // Current date
        let now = Utc::now().date_naive();
        return Ok(Value::String(now.format("%Y-%m-%d").to_string()));
    }

    match &args[0] {
        Value::String(s) => {
            let date = parse_date_string(s)?;
            Ok(Value::String(date.format("%Y-%m-%d").to_string()))
        }
        Value::Map(map) => eval_date_from_map(map),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("date() expects a string or map argument")),
    }
}

fn eval_date_from_map(map: &HashMap<String, Value>) -> Result<Value> {
    // Check if we have a 'date' field to copy from another date/datetime
    if let Some(dt_val) = map.get("date") {
        return eval_date_from_projection(map, dt_val);
    }

    let date = build_date_from_map(map)?;
    Ok(Value::String(date.format("%Y-%m-%d").to_string()))
}

/// Handle date construction from projection (copying from another temporal value).
fn eval_date_from_projection(map: &HashMap<String, Value>, source: &Value) -> Result<Value> {
    let source_str = source
        .as_str()
        .ok_or_else(|| anyhow!("date field must be a string"))?;
    let (source_date, _, _) = parse_datetime_with_tz(source_str)?;
    let date = build_date_from_projection(map, &source_date)?;
    Ok(Value::String(date.format("%Y-%m-%d").to_string()))
}

/// Build a NaiveDate from projection map, using source_date for defaults.
///
/// Supports multiple override modes:
/// - Week-based: override week, dayOfWeek (uses weekYear from source)
/// - Ordinal: override ordinalDay (uses year from source)
/// - Quarter: override quarter, dayOfQuarter (uses year from source)
/// - Calendar: override year, month, day (defaults from source)
fn build_date_from_projection(
    map: &HashMap<String, Value>,
    source_date: &NaiveDate,
) -> Result<NaiveDate> {
    // Week-based: {date: other, week: 2, dayOfWeek: 3}
    if map.contains_key("week") {
        let week_year = map
            .get("weekYear")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32)
            .unwrap_or_else(|| source_date.iso_week().year());
        let week = map.get("week").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
        let dow = map
            .get("dayOfWeek")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| source_date.weekday().number_from_monday() as i64)
            as u32;
        return build_date_from_week(week_year, week, dow);
    }

    // Ordinal: {date: other, ordinalDay: 202}
    if map.contains_key("ordinalDay") {
        let year = map
            .get("year")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32)
            .unwrap_or(source_date.year());
        let ordinal = map
            .get("ordinalDay")
            .and_then(|v| v.as_i64())
            .unwrap_or(source_date.ordinal() as i64) as u32;
        return NaiveDate::from_yo_opt(year, ordinal)
            .ok_or_else(|| anyhow!("Invalid ordinal day: {} for year {}", ordinal, year));
    }

    // Quarter: {date: other, quarter: 3, dayOfQuarter: 45}
    if map.contains_key("quarter") {
        let year = map
            .get("year")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32)
            .unwrap_or(source_date.year());
        let quarter = map.get("quarter").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
        let doq = map
            .get("dayOfQuarter")
            .and_then(|v| v.as_i64())
            .unwrap_or(1) as u32;
        return build_date_from_quarter(year, quarter, doq);
    }

    // Calendar-based: year, month, day with defaults from source
    let year = map
        .get("year")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32)
        .unwrap_or(source_date.year());
    let month = map
        .get("month")
        .and_then(|v| v.as_i64())
        .map(|v| v as u32)
        .unwrap_or(source_date.month());
    let day = map
        .get("day")
        .and_then(|v| v.as_i64())
        .map(|v| v as u32)
        .unwrap_or(source_date.day());

    NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| anyhow!("Invalid date in projection"))
}

/// Build a NaiveDate from map fields.
///
/// Supports multiple construction modes:
/// - Calendar: year, month, day
/// - Week-based: year, week, dayOfWeek
/// - Ordinal: year, ordinalDay
/// - Quarter: year, quarter, dayOfQuarter
fn build_date_from_map(map: &HashMap<String, Value>) -> Result<NaiveDate> {
    // Extract year (required for all date map constructors)
    let year = map
        .get("year")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("date/datetime map requires 'year' field"))? as i32;

    // Week-based: {year: 1984, week: 10, dayOfWeek: 3}
    if let Some(week) = map.get("week").and_then(|v| v.as_i64()) {
        let dow = map.get("dayOfWeek").and_then(|v| v.as_i64()).unwrap_or(1);
        return build_date_from_week(year, week as u32, dow as u32);
    }

    // Ordinal: {year: 1984, ordinalDay: 202}
    if let Some(ordinal) = map.get("ordinalDay").and_then(|v| v.as_i64()) {
        return NaiveDate::from_yo_opt(year, ordinal as u32)
            .ok_or_else(|| anyhow!("Invalid ordinal day: {} for year {}", ordinal, year));
    }

    // Quarter: {year: 1984, quarter: 3, dayOfQuarter: 45}
    if let Some(quarter) = map.get("quarter").and_then(|v| v.as_i64()) {
        let doq = map
            .get("dayOfQuarter")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        return build_date_from_quarter(year, quarter as u32, doq as u32);
    }

    // Calendar: standard year/month/day (with defaults)
    let month = map.get("month").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
    let day = map.get("day").and_then(|v| v.as_i64()).unwrap_or(1) as u32;

    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| anyhow!("Invalid date: year={}, month={}, day={}", year, month, day))
}

/// Build date from ISO week number (returns NaiveDate).
fn build_date_from_week(year: i32, week: u32, day_of_week: u32) -> Result<NaiveDate> {
    if !(1..=53).contains(&week) {
        return Err(anyhow!("Week must be between 1 and 53"));
    }
    if !(1..=7).contains(&day_of_week) {
        return Err(anyhow!("Day of week must be between 1 and 7"));
    }

    // Find January 4th of the given year (always in week 1)
    let jan4 =
        NaiveDate::from_ymd_opt(year, 1, 4).ok_or_else(|| anyhow!("Invalid year: {}", year))?;

    // Find Monday of week 1
    let iso_week_day = jan4.weekday().num_days_from_monday();
    let week1_monday = jan4 - Duration::days(iso_week_day as i64);

    // Calculate target date
    let days_offset = ((week - 1) * 7 + (day_of_week - 1)) as i64;
    Ok(week1_monday + Duration::days(days_offset))
}

/// Build date from quarter and day of quarter (returns NaiveDate).
fn build_date_from_quarter(year: i32, quarter: u32, day_of_quarter: u32) -> Result<NaiveDate> {
    if !(1..=4).contains(&quarter) {
        return Err(anyhow!("Quarter must be between 1 and 4"));
    }

    // First day of quarter
    let first_month = (quarter - 1) * 3 + 1;
    let quarter_start = NaiveDate::from_ymd_opt(year, first_month, 1)
        .ok_or_else(|| anyhow!("Invalid quarter start"))?;

    // Add days (day_of_quarter is 1-based)
    let result = quarter_start + Duration::days((day_of_quarter - 1) as i64);

    // Validate the result is still in the same quarter
    let result_quarter = (result.month() - 1) / 3 + 1;
    if result_quarter != quarter || result.year() != year {
        return Err(anyhow!(
            "Day {} is out of range for quarter {}",
            day_of_quarter,
            quarter
        ));
    }

    Ok(result)
}

fn parse_date_string(s: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| dt.date()))
        .or_else(|_| {
            // Try parsing RFC3339 datetime and extract date
            DateTime::parse_from_rfc3339(s).map(|dt| dt.date_naive())
        })
        .map_err(|e| anyhow!("Invalid date format: {}", e))
}

// ============================================================================
// Time Constructors
// ============================================================================

fn eval_time(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        let now = Utc::now();
        let time = now.time();
        return Ok(Value::String(format!(
            "{}+00:00",
            format_time_with_nanos(&time)
        )));
    }

    match &args[0] {
        Value::String(s) => {
            // Use parse_datetime_with_tz to handle timezone offsets like +0100.
            let (_, time, tz_info) = parse_datetime_with_tz(s)?;
            match tz_info {
                Some(ref info) => {
                    let offset_str = format_timezone_offset(
                        info.offset_for_local(&NaiveDateTime::new(Utc::now().date_naive(), time))?
                            .local_minus_utc(),
                    );
                    Ok(Value::String(format!(
                        "{}{}",
                        format_time_with_nanos(&time),
                        offset_str
                    )))
                }
                None => {
                    // No timezone info — just return time with +00:00
                    Ok(Value::String(format!(
                        "{}+00:00",
                        format_time_with_nanos(&time)
                    )))
                }
            }
        }
        Value::Map(map) => eval_time_from_map(map, true),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("time() expects a string or map argument")),
    }
}

fn eval_localtime(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        let now = chrono::Local::now().time();
        return Ok(Value::String(format_time_with_nanos(&now)));
    }

    match &args[0] {
        Value::String(s) => {
            let time = parse_time_string(s)?;
            Ok(Value::String(format_time_with_nanos(&time)))
        }
        Value::Map(map) => eval_time_from_map(map, false),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("localtime() expects a string or map argument")),
    }
}

fn eval_time_from_map(map: &HashMap<String, Value>, with_timezone: bool) -> Result<Value> {
    // Check if we have a 'time' field to copy from another time/datetime
    if let Some(time_val) = map.get("time") {
        return eval_time_from_projection(map, time_val, with_timezone);
    }

    let hour = map.get("hour").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let minute = map.get("minute").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let second = map.get("second").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let nanos = build_nanoseconds(map);

    let time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos).ok_or_else(|| {
        anyhow!(
            "Invalid time: hour={}, minute={}, second={}",
            hour,
            minute,
            second
        )
    })?;

    // Handle timezone for time() if present
    if let Some(tz_str) = map.get("timezone").and_then(|v| v.as_str()) {
        return format_time_with_timezone(&time, tz_str);
    }

    Ok(Value::String(format_time_with_nanos(&time)))
}

/// Handle time construction from projection (copying from another temporal value).
fn eval_time_from_projection(
    map: &HashMap<String, Value>,
    source: &Value,
    with_timezone: bool,
) -> Result<Value> {
    let source_str = source
        .as_str()
        .ok_or_else(|| anyhow!("time field must be a string"))?;

    // Parse the source time
    let (_, source_time, source_tz) = parse_datetime_with_tz(source_str)?;

    // Apply overrides from the map
    let hour = map
        .get("hour")
        .and_then(|v| v.as_i64())
        .map(|v| v as u32)
        .unwrap_or(source_time.hour());
    let minute = map
        .get("minute")
        .and_then(|v| v.as_i64())
        .map(|v| v as u32)
        .unwrap_or(source_time.minute());
    let second = map
        .get("second")
        .and_then(|v| v.as_i64())
        .map(|v| v as u32)
        .unwrap_or(source_time.second());

    let nanos = if map.contains_key("millisecond")
        || map.contains_key("microsecond")
        || map.contains_key("nanosecond")
    {
        build_nanoseconds(map)
    } else {
        source_time.nanosecond()
    };

    let time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)
        .ok_or_else(|| anyhow!("Invalid time in projection"))?;

    if with_timezone {
        // Use timezone from map if provided, otherwise from source
        if let Some(tz_str) = map.get("timezone").and_then(|v| v.as_str()) {
            return format_time_with_timezone(&time, tz_str);
        }

        if let Some(tz) = source_tz {
            // Format with source timezone
            let today = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let ndt = NaiveDateTime::new(today, time);
            let offset = tz.offset_for_local(&ndt)?;
            let secs = offset.local_minus_utc();
            let offset_str = format_timezone_offset(secs);
            let time_str = format_time_with_nanos(&time);
            return Ok(Value::String(format!("{}{}", time_str, offset_str)));
        }
    }

    Ok(Value::String(format_time_with_nanos(&time)))
}

fn parse_time_string(s: &str) -> Result<NaiveTime> {
    // Try various time formats
    NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.9f"))
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| dt.time()))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").map(|dt| dt.time()))
        .or_else(|_| DateTime::parse_from_rfc3339(s).map(|dt| dt.time()))
        .map_err(|_| anyhow!("Invalid time format"))
}

fn build_nanoseconds(map: &HashMap<String, Value>) -> u32 {
    let millis = map.get("millisecond").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let micros = map.get("microsecond").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let nanos = map.get("nanosecond").and_then(|v| v.as_i64()).unwrap_or(0) as u32;

    millis * 1_000_000 + micros * 1_000 + nanos
}

/// Format timezone offset with optional seconds (e.g., "+01:00" or "+02:05:59").
fn format_timezone_offset(offset_secs: i32) -> String {
    if offset_secs == 0 {
        "Z".to_string()
    } else {
        let hours = offset_secs / 3600;
        let remaining = offset_secs.abs() % 3600;
        let mins = remaining / 60;
        let secs = remaining % 60;
        if secs != 0 {
            format!("{:+03}:{:02}:{:02}", hours, mins, secs)
        } else {
            format!("{:+03}:{:02}", hours, mins)
        }
    }
}

fn format_time_with_nanos(time: &NaiveTime) -> String {
    let nanos = time.nanosecond();
    let secs = time.second();

    if nanos == 0 && secs == 0 {
        // Omit :00 seconds when they're zero
        time.format("%H:%M").to_string()
    } else if nanos == 0 {
        time.format("%H:%M:%S").to_string()
    } else if nanos.is_multiple_of(1_000_000) {
        // Milliseconds only
        time.format("%H:%M:%S%.3f").to_string()
    } else if nanos.is_multiple_of(1_000) {
        // Microseconds
        time.format("%H:%M:%S%.6f").to_string()
    } else {
        // Full nanoseconds
        time.format("%H:%M:%S%.9f").to_string()
    }
}

fn format_time_with_timezone(time: &NaiveTime, tz_str: &str) -> Result<Value> {
    // Parse timezone offset like "+01:00", "-05:00", "Z", "+02:05:59"
    let offset = parse_timezone_offset(tz_str)?;
    let offset_str = format_timezone_offset(offset);

    let time_str = format_time_with_nanos(time);
    Ok(Value::String(format!("{}{}", time_str, offset_str)))
}

fn parse_timezone_offset(tz: &str) -> Result<i32> {
    let tz = tz.trim();
    if tz == "Z" || tz == "z" {
        return Ok(0);
    }

    // Parse +HH:MM or -HH:MM or +HH:MM:SS format
    if tz.len() >= 5 && (tz.starts_with('+') || tz.starts_with('-')) {
        let sign = if tz.starts_with('-') { -1 } else { 1 };
        let hours: i32 = tz[1..3]
            .parse()
            .map_err(|_| anyhow!("Invalid timezone hours"))?;
        let mins: i32 = if tz.len() >= 6 {
            tz[4..6]
                .parse()
                .map_err(|_| anyhow!("Invalid timezone minutes"))?
        } else {
            0
        };
        let secs: i32 = if tz.len() >= 9 {
            // Format is +HH:MM:SS
            tz[7..9]
                .parse()
                .map_err(|_| anyhow!("Invalid timezone seconds"))?
        } else {
            0
        };
        return Ok(sign * (hours * 3600 + mins * 60 + secs));
    }

    Err(anyhow!("Unsupported timezone format: {}", tz))
}

// ============================================================================
// Datetime Constructors
// ============================================================================

fn eval_datetime(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        let now = Utc::now();
        return Ok(Value::String(format_datetime_with_nanos(&now)));
    }

    match &args[0] {
        Value::String(s) => {
            // Use the comprehensive parser that handles +0200, [Europe/Stockholm], etc.
            let (date, time, tz_info) = parse_datetime_with_tz(s)?;
            let ndt = NaiveDateTime::new(date, time);
            match tz_info {
                Some(ref info) => {
                    let fo = info.offset_for_local(&ndt)?;
                    let dt = fo
                        .from_local_datetime(&ndt)
                        .single()
                        .unwrap_or_else(|| fo.from_utc_datetime(&ndt));
                    Ok(Value::String(format_datetime_with_offset_and_tz(
                        &dt,
                        info.name(),
                    )))
                }
                None => {
                    // No timezone — treat as UTC
                    let dt = Utc.from_utc_datetime(&ndt);
                    Ok(Value::String(format_datetime_with_nanos(&dt)))
                }
            }
        }
        Value::Map(map) => eval_datetime_from_map(map, true),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("datetime() expects a string or map argument")),
    }
}

fn eval_localdatetime(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        let now = chrono::Local::now();
        return Ok(Value::String(format_datetime_local(&now)));
    }

    match &args[0] {
        Value::String(s) => {
            // Use parse_datetime_with_tz for comprehensive format support, then
            // discard any timezone info since this is local datetime.
            let (date, time, _) = parse_datetime_with_tz(s)?;
            let ndt = NaiveDateTime::new(date, time);
            Ok(Value::String(format_naive_datetime(&ndt)))
        }
        Value::Map(map) => eval_datetime_from_map(map, false),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("localdatetime() expects a string or map argument")),
    }
}

fn eval_datetime_from_map(map: &HashMap<String, Value>, with_timezone: bool) -> Result<Value> {
    // Check if we have a 'datetime' field to copy from another datetime
    if let Some(dt_val) = map.get("datetime").or(map.get("date")) {
        return eval_datetime_from_projection(map, dt_val, with_timezone);
    }

    // Build time part (used by all date construction methods)
    let hour = map.get("hour").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let minute = map.get("minute").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let second = map.get("second").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let nanos = build_nanoseconds(map);

    let time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)
        .ok_or_else(|| anyhow!("Invalid time in datetime map"))?;

    // Build date part - support multiple construction modes
    let date = build_date_from_map(map)?;

    let ndt = NaiveDateTime::new(date, time);

    if with_timezone {
        // Handle timezone
        if let Some(tz_str) = map.get("timezone").and_then(|v| v.as_str()) {
            let tz_info = parse_timezone(tz_str)?;
            let offset = tz_info.offset_for_local(&ndt)?;
            let dt = offset
                .from_local_datetime(&ndt)
                .single()
                .ok_or_else(|| anyhow!("Ambiguous or invalid local time"))?;
            return Ok(Value::String(format_datetime_with_offset_and_tz(
                &dt,
                tz_info.name(),
            )));
        }

        // Default to UTC
        let dt = DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc);
        Ok(Value::String(format_datetime_with_nanos(&dt)))
    } else {
        // localdatetime - no timezone
        Ok(Value::String(format_naive_datetime(&ndt)))
    }
}

/// Handle datetime construction from projection (copying from another temporal value).
fn eval_datetime_from_projection(
    map: &HashMap<String, Value>,
    source: &Value,
    with_timezone: bool,
) -> Result<Value> {
    let source_str = source
        .as_str()
        .ok_or_else(|| anyhow!("datetime/date field must be a string"))?;

    let (source_date, source_time, source_tz) = parse_datetime_with_tz(source_str)?;

    // Build date portion using shared helper
    let date = build_date_from_projection(map, &source_date)?;

    // Build time portion
    let hour = map
        .get("hour")
        .and_then(|v| v.as_i64())
        .map(|v| v as u32)
        .unwrap_or(source_time.hour());
    let minute = map
        .get("minute")
        .and_then(|v| v.as_i64())
        .map(|v| v as u32)
        .unwrap_or(source_time.minute());
    let second = map
        .get("second")
        .and_then(|v| v.as_i64())
        .map(|v| v as u32)
        .unwrap_or(source_time.second());

    let nanos = if map.contains_key("millisecond")
        || map.contains_key("microsecond")
        || map.contains_key("nanosecond")
    {
        build_nanoseconds(map)
    } else {
        source_time.nanosecond()
    };

    let time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)
        .ok_or_else(|| anyhow!("Invalid time in projection"))?;

    let ndt = NaiveDateTime::new(date, time);

    if with_timezone {
        // Use timezone from map if provided, otherwise from source
        let tz_info = if let Some(tz_str) = map.get("timezone").and_then(|v| v.as_str()) {
            Some(parse_timezone(tz_str)?)
        } else {
            source_tz
        };

        if let Some(ref tz) = tz_info {
            let offset = tz.offset_for_local(&ndt)?;
            let dt = offset
                .from_local_datetime(&ndt)
                .single()
                .ok_or_else(|| anyhow!("Ambiguous or invalid local time"))?;
            return Ok(Value::String(format_datetime_with_offset_and_tz(
                &dt,
                tz.name(),
            )));
        }

        // Default to UTC
        let dt = DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc);
        Ok(Value::String(format_datetime_with_nanos(&dt)))
    } else {
        Ok(Value::String(format_naive_datetime(&ndt)))
    }
}

/// Try parsing a string as a NaiveTime using common formats (%H:%M:%S%.f, %H:%M:%S, %H:%M).
fn try_parse_naive_time(s: &str) -> Result<NaiveTime, chrono::ParseError> {
    NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M"))
}

/// Try parsing a string as a NaiveDateTime using common ISO formats.
fn try_parse_naive_datetime(s: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M"))
}

/// Parse a datetime string and extract date, time, and timezone info.
pub fn parse_datetime_with_tz(s: &str) -> Result<(NaiveDate, NaiveTime, Option<TimezoneInfo>)> {
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    let today = Utc::now().date_naive();

    // Check for named timezone suffix like [Europe/Stockholm]
    let (datetime_part, tz_name) = if let Some(bracket_pos) = s.find('[') {
        let tz_name = s[bracket_pos + 1..s.len() - 1].to_string();
        (&s[..bracket_pos], Some(tz_name))
    } else {
        (s, None)
    };

    // Try parsing as full datetime with timezone
    if let Ok(dt) = DateTime::parse_from_rfc3339(datetime_part) {
        let tz_info = if let Some(name) = tz_name {
            Some(parse_timezone(&name)?)
        } else {
            Some(TimezoneInfo::FixedOffset(dt.offset().fix()))
        };
        return Ok((dt.date_naive(), dt.time(), tz_info));
    }

    // Try various datetime formats
    if let Ok(ndt) = try_parse_naive_datetime(datetime_part) {
        let tz_info = tz_name.map(|n| parse_timezone(&n)).transpose()?;
        return Ok((ndt.date(), ndt.time(), tz_info));
    }

    // Date only
    if let Ok(d) = NaiveDate::parse_from_str(datetime_part, "%Y-%m-%d") {
        let tz_info = tz_name.map(|n| parse_timezone(&n)).transpose()?;
        return Ok((d, midnight, tz_info));
    }

    // Try parsing as datetime or time with non-RFC3339 timezone offset
    // (e.g., "2015-07-21T21:40:32.142+0100" or "12:31:14.645876123+01:00")
    if let Some(tz_pos) = datetime_part.rfind('+').or_else(|| {
        // Find the last '-' that's part of timezone, not time/date.
        // For datetimes like "2015-07-21T21:40:32-0500", the T is at pos 10;
        // the tz '-' must be after the seconds (pos >= 19). For time-only
        // like "12:31:14-05:00", the tz '-' is at pos 8. We require pos > 5
        // to skip "HH:MM" hyphens while catching both cases.
        datetime_part.rfind('-').filter(|&pos| {
            if let Some(t_pos) = datetime_part.find('T') {
                // Datetime: '-' must be well after the T and seconds
                pos > t_pos + 6
            } else {
                // Time-only: '-' must be after at least HH:MM:SS
                pos > 5
            }
        })
    }) {
        let left_part = &datetime_part[..tz_pos];
        let tz_part = &datetime_part[tz_pos..];

        let resolve_tz = |tz_name: Option<String>, tz_part: &str| -> Result<Option<TimezoneInfo>> {
            if let Some(name) = tz_name {
                Ok(Some(parse_timezone(&name)?))
            } else {
                let offset = parse_timezone_offset(tz_part)?;
                let fo = FixedOffset::east_opt(offset)
                    .ok_or_else(|| anyhow!("Invalid timezone offset"))?;
                Ok(Some(TimezoneInfo::FixedOffset(fo)))
            }
        };

        // Try parsing the left part as a full datetime first
        if let Ok(ndt) = try_parse_naive_datetime(left_part) {
            let tz_info = resolve_tz(tz_name, tz_part)?;
            return Ok((ndt.date(), ndt.time(), tz_info));
        }

        // Try parsing the left part as time only
        if let Ok(time) = try_parse_naive_time(left_part) {
            let tz_info = resolve_tz(tz_name, tz_part)?;
            return Ok((today, time, tz_info));
        }
    }

    // Try parsing time with Z suffix
    if let Some(time_part) = datetime_part.strip_suffix('Z')
        && let Ok(time) = try_parse_naive_time(time_part)
    {
        let tz_info = Some(TimezoneInfo::FixedOffset(FixedOffset::east_opt(0).unwrap()));
        return Ok((today, time, tz_info));
    }

    // Try parsing as plain time (no timezone offset, e.g., "14:30" or "12:31:14.645876123")
    if let Ok(time) = try_parse_naive_time(datetime_part) {
        let tz_info = tz_name.map(|n| parse_timezone(&n)).transpose()?;
        return Ok((today, time, tz_info));
    }

    Err(anyhow!("Cannot parse datetime: {}", s))
}

/// Select the chrono format string for the time portion based on nanosecond precision.
fn nanos_precision_format(nanos: u32, seconds: u32) -> &'static str {
    if nanos == 0 && seconds == 0 {
        "%Y-%m-%dT%H:%M"
    } else if nanos == 0 {
        "%Y-%m-%dT%H:%M:%S"
    } else if nanos.is_multiple_of(1_000_000) {
        "%Y-%m-%dT%H:%M:%S%.3f"
    } else if nanos.is_multiple_of(1_000) {
        "%Y-%m-%dT%H:%M:%S%.6f"
    } else {
        "%Y-%m-%dT%H:%M:%S%.9f"
    }
}

fn format_datetime_with_nanos(dt: &DateTime<Utc>) -> String {
    let fmt = nanos_precision_format(dt.nanosecond(), dt.second());
    format!("{}Z", dt.format(fmt))
}

fn format_datetime_with_offset_and_tz(dt: &DateTime<FixedOffset>, tz_name: Option<&str>) -> String {
    let fmt = nanos_precision_format(dt.nanosecond(), dt.second());
    let tz_suffix = format_timezone_offset(dt.offset().local_minus_utc());
    let base = format!("{}{}", dt.format(fmt), tz_suffix);

    if let Some(name) = tz_name {
        format!("{}[{}]", base, name)
    } else {
        base
    }
}

fn format_datetime_local(dt: &DateTime<chrono::Local>) -> String {
    let fmt = nanos_precision_format(dt.nanosecond(), dt.second());
    let tz_suffix = format_timezone_offset(dt.offset().local_minus_utc());
    format!("{}{}", dt.format(fmt), tz_suffix)
}

fn format_naive_datetime(ndt: &NaiveDateTime) -> String {
    let fmt = nanos_precision_format(ndt.nanosecond(), ndt.second());
    ndt.format(fmt).to_string()
}

// ============================================================================
// CypherDuration for ISO 8601 formatting
// ============================================================================

/// Represents a Cypher duration with separate month, day, and nanosecond components.
///
/// This allows proper ISO 8601 formatting without loss of calendar semantics.
#[derive(Debug, Clone, PartialEq)]
pub struct CypherDuration {
    /// Months (includes years * 12)
    pub months: i64,
    /// Days (includes weeks * 7)
    pub days: i64,
    /// Nanoseconds (time portion only, excludes days)
    pub nanos: i64,
}

impl CypherDuration {
    pub fn new(months: i64, days: i64, nanos: i64) -> Self {
        Self {
            months,
            days,
            nanos,
        }
    }

    /// Create from total microseconds (loses calendar semantics).
    pub fn from_micros(micros: i64) -> Self {
        let total_nanos = micros * 1000;
        let total_secs = total_nanos / NANOS_PER_SECOND;
        let remaining_nanos = total_nanos % NANOS_PER_SECOND;

        let days = total_secs / (24 * 3600);
        let day_secs = total_secs % (24 * 3600);

        Self {
            months: 0,
            days,
            nanos: day_secs * NANOS_PER_SECOND + remaining_nanos,
        }
    }

    /// Format as ISO 8601 duration string.
    ///
    /// Handles negative components and mixed-sign seconds/nanoseconds correctly.
    pub fn to_iso8601(&self) -> String {
        let mut result = String::from("P");

        let years = self.months / 12;
        let months = self.months % 12;

        if years != 0 {
            result.push_str(&format!("{}Y", years));
        }
        if months != 0 {
            result.push_str(&format!("{}M", months));
        }
        if self.days != 0 {
            result.push_str(&format!("{}D", self.days));
        }

        // Time part: use truncating division (towards zero) so each component
        // independently carries its sign, matching Neo4j's format.
        let nanos = self.nanos;
        let total_secs = nanos / NANOS_PER_SECOND; // truncates towards zero
        let remaining_nanos = nanos % NANOS_PER_SECOND; // same sign as nanos

        let hours = total_secs / 3600;
        let rem_after_hours = total_secs % 3600;
        let minutes = rem_after_hours / 60;
        let seconds = rem_after_hours % 60;

        if hours != 0 || minutes != 0 || seconds != 0 || remaining_nanos != 0 {
            result.push('T');

            if hours != 0 {
                result.push_str(&format!("{}H", hours));
            }
            if minutes != 0 {
                result.push_str(&format!("{}M", minutes));
            }
            if seconds != 0 || remaining_nanos != 0 {
                if remaining_nanos != 0 {
                    // Combine seconds + remaining nanos into fractional seconds.
                    // Both have the same sign (truncating division preserves sign).
                    let secs_with_nanos = seconds as f64 + (remaining_nanos as f64 / 1e9);
                    let formatted = format!("{:.9}", secs_with_nanos);
                    let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
                    result.push_str(trimmed);
                    result.push('S');
                } else {
                    result.push_str(&format!("{}S", seconds));
                }
            }
        }

        // Handle case where duration is zero
        if result == "P" {
            result.push_str("T0S");
        }

        result
    }

    /// Get total as microseconds (for arithmetic operations).
    pub fn to_micros(&self) -> i64 {
        let month_days = self.months * 30; // Approximate
        let total_days = month_days + self.days;
        let day_micros = total_days * MICROS_PER_DAY;
        let nano_micros = self.nanos / 1000;
        day_micros + nano_micros
    }

    /// Component-wise addition of two durations.
    pub fn add(&self, other: &CypherDuration) -> CypherDuration {
        CypherDuration::new(
            self.months + other.months,
            self.days + other.days,
            self.nanos + other.nanos,
        )
    }

    /// Component-wise subtraction of two durations.
    pub fn sub(&self, other: &CypherDuration) -> CypherDuration {
        CypherDuration::new(
            self.months - other.months,
            self.days - other.days,
            self.nanos - other.nanos,
        )
    }

    /// Negate all components.
    pub fn negate(&self) -> CypherDuration {
        CypherDuration::new(-self.months, -self.days, -self.nanos)
    }

    /// Multiply duration by a factor with fractional cascading.
    pub fn multiply(&self, factor: f64) -> CypherDuration {
        let months_f = self.months as f64 * factor;
        let whole_months = months_f.trunc() as i64;
        let frac_months = months_f.fract();

        // Cascade fractional months via average Gregorian month (2629746 seconds).
        let frac_month_seconds = frac_months * 2_629_746.0;
        let extra_days_from_months = (frac_month_seconds / SECONDS_PER_DAY as f64).trunc();
        let remaining_secs_from_months =
            frac_month_seconds - extra_days_from_months * SECONDS_PER_DAY as f64;

        let days_f = self.days as f64 * factor + extra_days_from_months;
        let whole_days = days_f.trunc() as i64;
        let frac_days = days_f.fract();

        let nanos_f = self.nanos as f64 * factor
            + remaining_secs_from_months * NANOS_PER_SECOND as f64
            + frac_days * NANOS_PER_DAY as f64;

        CypherDuration::new(whole_months, whole_days, nanos_f.trunc() as i64)
    }

    /// Divide duration by a divisor with fractional cascading.
    pub fn divide(&self, divisor: f64) -> CypherDuration {
        if divisor == 0.0 {
            // Return zero duration for division by zero (matches Cypher behavior)
            return CypherDuration::new(0, 0, 0);
        }
        self.multiply(1.0 / divisor)
    }
}

// ============================================================================
// Calendar-Aware Duration Arithmetic
// ============================================================================

/// Add months to a date with day-of-month clamping.
///
/// If the resulting month has fewer days than the source day,
/// the day is clamped to the last day of the month.
/// For example, `Jan 31 + 1 month = Feb 28` (or Feb 29 in leap years).
pub fn add_months_to_date(date: NaiveDate, months: i64) -> NaiveDate {
    if months == 0 {
        return date;
    }

    let total_months = date.year() as i64 * 12 + (date.month() as i64 - 1) + months;
    let new_year = total_months.div_euclid(12) as i32;
    let new_month = (total_months.rem_euclid(12) + 1) as u32;

    // Clamp day to valid range for the new month
    let max_day = days_in_month(new_year, new_month);
    let new_day = date.day().min(max_day);

    NaiveDate::from_ymd_opt(new_year, new_month, new_day)
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(new_year, new_month, 1).unwrap())
}

/// Get number of days in a given month.
fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// Add a CypherDuration to a date string, returning the result date string.
///
/// Algorithm: add months (clamping) -> add days -> add nanos (overflow into extra days).
pub fn add_cypher_duration_to_date(date_str: &str, dur: &CypherDuration) -> Result<String> {
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?;

    // Step 1: Add months with clamping
    let after_months = add_months_to_date(date, dur.months);

    // Step 2: Add days
    let after_days = after_months + Duration::days(dur.days);

    // Step 3: Add nanos (overflow into extra full days; sub-day remainder is discarded for dates)
    // Use truncating division (/) so negative sub-day nanos don't subtract an extra day.
    let extra_days = dur.nanos / NANOS_PER_DAY;
    let result = after_days + Duration::days(extra_days);

    Ok(result.format("%Y-%m-%d").to_string())
}

/// Add a CypherDuration to a local time string, returning the result time string.
///
/// Time wraps modulo 24 hours. No date component is affected.
pub fn add_cypher_duration_to_localtime(time_str: &str, dur: &CypherDuration) -> Result<String> {
    let time = parse_time_string(time_str)?;
    let total_nanos = time_to_nanos(&time) + dur.nanos;
    // Wrap modulo 24h
    let wrapped = total_nanos.rem_euclid(NANOS_PER_DAY);
    let result = nanos_to_time(wrapped);
    Ok(format_time_with_nanos(&result))
}

/// Add a CypherDuration to a time-with-timezone string, returning the result time string.
///
/// Time wraps modulo 24 hours, preserving the original timezone offset.
pub fn add_cypher_duration_to_time(time_str: &str, dur: &CypherDuration) -> Result<String> {
    let (_, time, tz_info) = parse_datetime_with_tz(time_str)?;
    let total_nanos = time_to_nanos(&time) + dur.nanos;
    let wrapped = total_nanos.rem_euclid(NANOS_PER_DAY);
    let result_time = nanos_to_time(wrapped);

    let time_part = format_time_with_nanos(&result_time);
    if let Some(ref tz) = tz_info {
        let today = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let ndt = NaiveDateTime::new(today, result_time);
        let offset = tz.offset_for_local(&ndt)?;
        let offset_str = format_timezone_offset(offset.local_minus_utc());
        Ok(format!("{}{}", time_part, offset_str))
    } else {
        Ok(time_part)
    }
}

/// Add a CypherDuration to a local datetime string, returning the result string.
pub fn add_cypher_duration_to_localdatetime(dt_str: &str, dur: &CypherDuration) -> Result<String> {
    let ndt = NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M"))
        .map_err(|_| anyhow!("Invalid localdatetime: {}", dt_str))?;

    // Step 1: Add months with clamping
    let after_months = add_months_to_date(ndt.date(), dur.months);
    // Step 2: Add days
    let after_days = after_months + Duration::days(dur.days);
    // Step 3: Add nanos to time
    let result_ndt = NaiveDateTime::new(after_days, ndt.time()) + Duration::nanoseconds(dur.nanos);

    Ok(format_naive_datetime(&result_ndt))
}

/// Add a CypherDuration to a datetime-with-timezone string, returning the result string.
pub fn add_cypher_duration_to_datetime(dt_str: &str, dur: &CypherDuration) -> Result<String> {
    let (date, time, tz_info) = parse_datetime_with_tz(dt_str)?;

    // Step 1: Add months with clamping
    let after_months = add_months_to_date(date, dur.months);
    // Step 2: Add days
    let after_days = after_months + Duration::days(dur.days);
    // Step 3: Add nanos to the datetime
    let ndt = NaiveDateTime::new(after_days, time) + Duration::nanoseconds(dur.nanos);

    if let Some(ref tz) = tz_info {
        let offset = tz.offset_for_local(&ndt)?;
        let dt = offset
            .from_local_datetime(&ndt)
            .single()
            .ok_or_else(|| anyhow!("Ambiguous local time after duration addition"))?;
        Ok(format_datetime_with_offset_and_tz(&dt, tz.name()))
    } else {
        let dt = DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc);
        Ok(format_datetime_with_nanos(&dt))
    }
}

/// Convert NaiveTime to total nanoseconds since midnight.
fn time_to_nanos(t: &NaiveTime) -> i64 {
    t.hour() as i64 * 3_600 * NANOS_PER_SECOND
        + t.minute() as i64 * 60 * NANOS_PER_SECOND
        + t.second() as i64 * NANOS_PER_SECOND
        + t.nanosecond() as i64
}

/// Convert total nanoseconds since midnight to NaiveTime.
fn nanos_to_time(nanos: i64) -> NaiveTime {
    let total_secs = nanos / NANOS_PER_SECOND;
    let remaining_nanos = (nanos % NANOS_PER_SECOND) as u32;
    let h = (total_secs / 3600) as u32;
    let m = ((total_secs % 3600) / 60) as u32;
    let s = (total_secs % 60) as u32;
    NaiveTime::from_hms_nano_opt(h, m, s, remaining_nanos)
        .unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).unwrap())
}

// ============================================================================
// Duration Constructor
// ============================================================================

fn eval_duration(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("duration() requires 1 argument"));
    }

    match &args[0] {
        Value::String(s) => {
            // Parse and return as ISO 8601 string
            let duration = parse_duration_to_cypher(s)?;
            Ok(Value::String(duration.to_iso8601()))
        }
        Value::Map(map) => eval_duration_from_map(map),
        Value::Int(_) | Value::Float(_) => {
            // Treat as microseconds, convert to ISO 8601
            if let Some(micros) = args[0].as_i64() {
                let duration = CypherDuration::from_micros(micros);
                Ok(Value::String(duration.to_iso8601()))
            } else {
                Ok(args[0].clone())
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("duration() expects a string, map, or number")),
    }
}

/// Build duration from a map with fractional cascading.
///
/// Fractional parts cascade to the next smaller unit:
/// - `months: 5.5` -> 5 months + 15 days (0.5 * 30)
/// - `days: 14.5` -> 14 days + 12 hours (0.5 * 24h in nanos)
fn eval_duration_from_map(map: &HashMap<String, Value>) -> Result<Value> {
    let mut months_f: f64 = 0.0;
    let mut days_f: f64 = 0.0;
    let mut nanos_f: f64 = 0.0;

    // Calendar components with fractional cascading
    if let Some(years) = map.get("years").and_then(get_numeric_value) {
        months_f += years * 12.0;
    }
    if let Some(m) = map.get("months").and_then(get_numeric_value) {
        months_f += m;
    }

    // Cascade fractional months to days + remaining nanos.
    // Neo4j uses average Gregorian month: 2629746 seconds (= 365.2425 * 86400 / 12).
    let whole_months = months_f.trunc() as i64;
    let frac_months = months_f.fract();
    let frac_month_seconds = frac_months * 2_629_746.0;
    let extra_days_from_months = (frac_month_seconds / SECONDS_PER_DAY as f64).trunc();
    let remaining_secs_from_months =
        frac_month_seconds - extra_days_from_months * SECONDS_PER_DAY as f64;
    days_f += extra_days_from_months;
    nanos_f += remaining_secs_from_months * NANOS_PER_SECOND as f64;

    if let Some(weeks) = map.get("weeks").and_then(get_numeric_value) {
        days_f += weeks * 7.0;
    }
    if let Some(d) = map.get("days").and_then(get_numeric_value) {
        days_f += d;
    }

    // Cascade fractional days to nanos (1 day = 24h in nanos)
    let whole_days = days_f.trunc() as i64;
    let frac_days = days_f.fract();
    nanos_f += frac_days * NANOS_PER_DAY as f64;

    // Time components (stored as nanoseconds)
    if let Some(hours) = map.get("hours").and_then(get_numeric_value) {
        nanos_f += hours * 3600.0 * NANOS_PER_SECOND as f64;
    }
    if let Some(minutes) = map.get("minutes").and_then(get_numeric_value) {
        nanos_f += minutes * 60.0 * NANOS_PER_SECOND as f64;
    }
    if let Some(seconds) = map.get("seconds").and_then(get_numeric_value) {
        nanos_f += seconds * NANOS_PER_SECOND as f64;
    }
    if let Some(millis) = map.get("milliseconds").and_then(get_numeric_value) {
        nanos_f += millis * 1_000_000.0;
    }
    if let Some(micros) = map.get("microseconds").and_then(get_numeric_value) {
        nanos_f += micros * 1_000.0;
    }
    if let Some(n) = map.get("nanoseconds").and_then(get_numeric_value) {
        nanos_f += n;
    }

    let duration = CypherDuration::new(whole_months, whole_days, nanos_f.trunc() as i64);
    Ok(Value::String(duration.to_iso8601()))
}

/// Extract numeric value from JSON, supporting both integers and floats.
fn get_numeric_value(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))
}

/// Parse ISO 8601 duration format (e.g., "P1DT2H30M15S").
fn parse_iso8601_duration(s: &str) -> Result<i64> {
    let s = &s[1..]; // Skip 'P'
    let mut total_micros: i64 = 0;
    let mut in_time_part = false;
    let mut num_buf = String::new();

    for c in s.chars() {
        if c == 'T' || c == 't' {
            in_time_part = true;
            continue;
        }

        if c.is_ascii_digit() || c == '.' || c == '-' {
            num_buf.push(c);
        } else {
            if num_buf.is_empty() {
                continue;
            }
            let num: f64 = num_buf
                .parse()
                .map_err(|_| anyhow!("Invalid duration number"))?;
            num_buf.clear();

            let micros = match c {
                'Y' | 'y' => (num * 365.0 * MICROS_PER_DAY as f64) as i64,
                'M' if !in_time_part => (num * 30.0 * MICROS_PER_DAY as f64) as i64, // Months
                'W' | 'w' => (num * 7.0 * MICROS_PER_DAY as f64) as i64,
                'D' | 'd' => (num * MICROS_PER_DAY as f64) as i64,
                'H' | 'h' => (num * MICROS_PER_HOUR as f64) as i64,
                'M' | 'm' if in_time_part => (num * MICROS_PER_MINUTE as f64) as i64, // Minutes
                'S' | 's' => (num * MICROS_PER_SECOND as f64) as i64,
                _ => return Err(anyhow!("Invalid ISO 8601 duration designator: {}", c)),
            };
            total_micros += micros;
        }
    }

    Ok(total_micros)
}

/// Parse simple duration format (e.g., "1d2h30m15s", "90s", "1h30m").
fn parse_simple_duration(s: &str) -> Result<i64> {
    let mut total_micros: i64 = 0;
    let mut num_buf = String::new();

    for c in s.chars() {
        if c.is_ascii_digit() || c == '.' || c == '-' {
            num_buf.push(c);
        } else if c.is_ascii_alphabetic() {
            if num_buf.is_empty() {
                return Err(anyhow!("Invalid duration format"));
            }
            let num: f64 = num_buf
                .parse()
                .map_err(|_| anyhow!("Invalid duration number"))?;
            num_buf.clear();

            let micros = match c {
                'w' => (num * 7.0 * MICROS_PER_DAY as f64) as i64,
                'd' => (num * MICROS_PER_DAY as f64) as i64,
                'h' => (num * MICROS_PER_HOUR as f64) as i64,
                'm' => (num * MICROS_PER_MINUTE as f64) as i64,
                's' => (num * MICROS_PER_SECOND as f64) as i64,
                _ => return Err(anyhow!("Invalid duration unit: {}", c)),
            };
            total_micros += micros;
        }
    }

    // Handle case where string is just a number (assume seconds)
    if !num_buf.is_empty() {
        let num: f64 = num_buf
            .parse()
            .map_err(|_| anyhow!("Invalid duration number"))?;
        total_micros += (num * MICROS_PER_SECOND as f64) as i64;
    }

    Ok(total_micros)
}

// ============================================================================
// Epoch Functions
// ============================================================================

fn eval_datetime_fromepoch(args: &[Value]) -> Result<Value> {
    let seconds = args
        .first()
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("datetime.fromepoch requires seconds argument"))?;
    let nanos = args.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as u32;

    let dt = DateTime::from_timestamp(seconds, nanos)
        .ok_or_else(|| anyhow!("Invalid epoch timestamp: {}", seconds))?;
    Ok(Value::String(format_datetime_with_nanos(&dt)))
}

fn eval_datetime_fromepochmillis(args: &[Value]) -> Result<Value> {
    let millis = args
        .first()
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("datetime.fromepochmillis requires milliseconds argument"))?;

    let dt = DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| anyhow!("Invalid epoch millis: {}", millis))?;

    Ok(Value::String(format_datetime_with_nanos(&dt)))
}

// ============================================================================
// Truncate Functions
// ============================================================================

fn eval_truncate(type_name: &str, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(anyhow!(
            "{}.truncate requires at least a unit argument",
            type_name
        ));
    }

    let unit = args
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("truncate requires unit as first argument"))?;

    let temporal = args.get(1);
    let adjust_map = args.get(2).and_then(|v| v.as_object());

    match type_name {
        "date" => truncate_date(unit, temporal, adjust_map),
        "time" => truncate_time(unit, temporal, adjust_map, true),
        "localtime" => truncate_time(unit, temporal, adjust_map, false),
        "datetime" | "localdatetime" => truncate_datetime(unit, temporal, adjust_map, type_name),
        _ => Err(anyhow!("Unknown truncate type: {}", type_name)),
    }
}

fn truncate_date(
    unit: &str,
    temporal: Option<&Value>,
    adjust_map: Option<&HashMap<String, Value>>,
) -> Result<Value> {
    let date = match temporal {
        Some(Value::String(s)) => parse_date_string(s)?,
        Some(Value::Null) | None => Utc::now().date_naive(),
        _ => return Err(anyhow!("truncate expects a date string")),
    };

    let truncated = truncate_date_to_unit(date, unit)?;

    if let Some(map) = adjust_map {
        apply_date_adjustments(truncated, map)
    } else {
        Ok(Value::String(truncated.format("%Y-%m-%d").to_string()))
    }
}

fn truncate_date_to_unit(date: NaiveDate, unit: &str) -> Result<NaiveDate> {
    let unit_lower = unit.to_lowercase();
    match unit_lower.as_str() {
        "millennium" => {
            // 2017 -> 2000, 1984 -> 1000, 999 -> 0
            let millennium_year = (date.year() / 1000) * 1000;
            NaiveDate::from_ymd_opt(millennium_year, 1, 1)
                .ok_or_else(|| anyhow!("Invalid millennium truncation"))
        }
        "century" => {
            // 1984 -> 1900, 2017 -> 2000
            let century_year = (date.year() / 100) * 100;
            NaiveDate::from_ymd_opt(century_year, 1, 1)
                .ok_or_else(|| anyhow!("Invalid century truncation"))
        }
        "decade" => {
            let decade_year = (date.year() / 10) * 10;
            NaiveDate::from_ymd_opt(decade_year, 1, 1)
                .ok_or_else(|| anyhow!("Invalid decade truncation"))
        }
        "year" => NaiveDate::from_ymd_opt(date.year(), 1, 1)
            .ok_or_else(|| anyhow!("Invalid year truncation")),
        "weekyear" => {
            // Truncate to first day of ISO week year
            let iso_week = date.iso_week();
            let week_year = iso_week.year();
            let jan4 =
                NaiveDate::from_ymd_opt(week_year, 1, 4).ok_or_else(|| anyhow!("Invalid date"))?;
            let iso_week_day = jan4.weekday().num_days_from_monday();
            Ok(jan4 - Duration::days(iso_week_day as i64))
        }
        "quarter" => {
            let quarter = (date.month() - 1) / 3;
            let first_month = quarter * 3 + 1;
            NaiveDate::from_ymd_opt(date.year(), first_month, 1)
                .ok_or_else(|| anyhow!("Invalid quarter truncation"))
        }
        "month" => NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
            .ok_or_else(|| anyhow!("Invalid month truncation")),
        "week" => {
            // Truncate to Monday of current week
            let weekday = date.weekday().num_days_from_monday();
            Ok(date - Duration::days(weekday as i64))
        }
        "day" => Ok(date),
        _ => Err(anyhow!("Unknown truncation unit for date: {}", unit)),
    }
}

fn apply_date_adjustments(date: NaiveDate, map: &HashMap<String, Value>) -> Result<Value> {
    let mut result = date;

    // Handle dayOfWeek adjustment (moves to different day in the same week)
    if let Some(dow) = map.get("dayOfWeek").and_then(|v| v.as_i64()) {
        // dayOfWeek: 1=Monday, 7=Sunday
        // Calculate the offset from Monday
        let current_dow = result.weekday().num_days_from_monday() as i64 + 1;
        let diff = dow - current_dow;
        result += Duration::days(diff);
    }

    if let Some(month) = map.get("month").and_then(|v| v.as_i64()) {
        result = NaiveDate::from_ymd_opt(result.year(), month as u32, result.day())
            .ok_or_else(|| anyhow!("Invalid month adjustment"))?;
    }
    if let Some(day) = map.get("day").and_then(|v| v.as_i64()) {
        result = NaiveDate::from_ymd_opt(result.year(), result.month(), day as u32)
            .ok_or_else(|| anyhow!("Invalid day adjustment"))?;
    }

    Ok(Value::String(result.format("%Y-%m-%d").to_string()))
}

fn truncate_time(
    unit: &str,
    temporal: Option<&Value>,
    adjust_map: Option<&HashMap<String, Value>>,
    with_timezone: bool,
) -> Result<Value> {
    let (date, time, tz_info) = match temporal {
        Some(Value::String(s)) => {
            // Try to parse as datetime/time with timezone first
            if let Ok((date, time, tz)) = parse_datetime_with_tz(s) {
                (date, time, tz)
            } else if let Ok(t) = parse_time_string(s) {
                // Use today for time-only parsing
                (Utc::now().date_naive(), t, None)
            } else {
                return Err(anyhow!("truncate expects a time string"));
            }
        }
        Some(Value::Null) | None => {
            let now = Utc::now();
            (now.date_naive(), now.time(), None)
        }
        _ => return Err(anyhow!("truncate expects a time string")),
    };

    let truncated = truncate_time_to_unit(time, unit)?;

    let final_time = if let Some(map) = adjust_map {
        apply_time_adjustments(truncated, map)?
    } else {
        truncated
    };

    // Format with or without timezone based on the output type
    if with_timezone {
        // time.truncate always outputs with timezone
        let offset_str = if let Some(ref tz) = tz_info {
            let offset_secs = tz.offset_seconds_with_date(&date);
            format_timezone_offset(offset_secs)
        } else {
            // Default to Z if no timezone in input
            "Z".to_string()
        };
        let time_str = format_time_with_nanos(&final_time);
        Ok(Value::String(format!("{}{}", time_str, offset_str)))
    } else {
        // localtime.truncate outputs without timezone
        Ok(Value::String(format_time_with_nanos(&final_time)))
    }
}

fn truncate_time_to_unit(time: NaiveTime, unit: &str) -> Result<NaiveTime> {
    let unit_lower = unit.to_lowercase();
    match unit_lower.as_str() {
        "day" => NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| anyhow!("Invalid truncation")),
        "hour" => {
            NaiveTime::from_hms_opt(time.hour(), 0, 0).ok_or_else(|| anyhow!("Invalid truncation"))
        }
        "minute" => NaiveTime::from_hms_opt(time.hour(), time.minute(), 0)
            .ok_or_else(|| anyhow!("Invalid truncation")),
        "second" => NaiveTime::from_hms_opt(time.hour(), time.minute(), time.second())
            .ok_or_else(|| anyhow!("Invalid truncation")),
        "millisecond" => {
            let millis = time.nanosecond() / 1_000_000;
            NaiveTime::from_hms_nano_opt(
                time.hour(),
                time.minute(),
                time.second(),
                millis * 1_000_000,
            )
            .ok_or_else(|| anyhow!("Invalid truncation"))
        }
        "microsecond" => {
            let micros = time.nanosecond() / 1_000;
            NaiveTime::from_hms_nano_opt(time.hour(), time.minute(), time.second(), micros * 1_000)
                .ok_or_else(|| anyhow!("Invalid truncation"))
        }
        _ => Err(anyhow!("Unknown truncation unit for time: {}", unit)),
    }
}

/// Apply time adjustments from a map and return the adjusted NaiveTime.
fn apply_time_adjustments(time: NaiveTime, map: &HashMap<String, Value>) -> Result<NaiveTime> {
    let hour = map
        .get("hour")
        .and_then(|v| v.as_i64())
        .unwrap_or(time.hour() as i64) as u32;
    let minute = map
        .get("minute")
        .and_then(|v| v.as_i64())
        .unwrap_or(time.minute() as i64) as u32;
    let second = map
        .get("second")
        .and_then(|v| v.as_i64())
        .unwrap_or(time.second() as i64) as u32;
    let nanos = build_nanoseconds(map);

    NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)
        .ok_or_else(|| anyhow!("Invalid time adjustment"))
}

fn truncate_datetime(
    unit: &str,
    temporal: Option<&Value>,
    adjust_map: Option<&HashMap<String, Value>>,
    type_name: &str,
) -> Result<Value> {
    let (date, time, tz_info) = match temporal {
        Some(Value::String(s)) => {
            // Use the new parser that preserves timezone info
            parse_datetime_with_tz(s)?
        }
        Some(Value::Null) | None => {
            let now = Utc::now();
            (
                now.date_naive(),
                now.time(),
                Some(TimezoneInfo::FixedOffset(FixedOffset::east_opt(0).unwrap())),
            )
        }
        _ => return Err(anyhow!("truncate expects a datetime string")),
    };

    // Check if adjustment map specifies a timezone
    let effective_tz = if let Some(map) = adjust_map {
        if let Some(tz_str) = map.get("timezone").and_then(|v| v.as_str()) {
            Some(parse_timezone(tz_str)?)
        } else {
            tz_info
        }
    } else {
        tz_info
    };

    // Truncate based on unit
    let (truncated_date, truncated_time) = truncate_datetime_to_unit(date, time, unit)?;

    if let Some(map) = adjust_map {
        apply_datetime_adjustments(
            truncated_date,
            truncated_time,
            map,
            type_name,
            effective_tz.as_ref(),
        )
    } else {
        let ndt = NaiveDateTime::new(truncated_date, truncated_time);
        if type_name == "localdatetime" {
            Ok(Value::String(format_naive_datetime(&ndt)))
        } else if let Some(ref tz) = effective_tz {
            let offset = tz.offset_for_local(&ndt)?;
            let dt = offset
                .from_local_datetime(&ndt)
                .single()
                .ok_or_else(|| anyhow!("Ambiguous local time"))?;
            Ok(Value::String(format_datetime_with_offset_and_tz(
                &dt,
                tz.name(),
            )))
        } else {
            let dt = DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc);
            Ok(Value::String(format_datetime_with_nanos(&dt)))
        }
    }
}

fn truncate_datetime_to_unit(
    date: NaiveDate,
    time: NaiveTime,
    unit: &str,
) -> Result<(NaiveDate, NaiveTime)> {
    let unit_lower = unit.to_lowercase();
    let midnight =
        NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| anyhow!("Failed to create midnight"))?;

    match unit_lower.as_str() {
        // Date-level truncations reset time to midnight
        "millennium" | "century" | "decade" | "year" | "weekyear" | "quarter" | "month"
        | "week" | "day" => {
            let truncated_date = truncate_date_to_unit(date, unit)?;
            Ok((truncated_date, midnight))
        }
        // Time-level truncations keep the date
        "hour" | "minute" | "second" | "millisecond" | "microsecond" => {
            let truncated_time = truncate_time_to_unit(time, unit)?;
            Ok((date, truncated_time))
        }
        _ => Err(anyhow!("Unknown truncation unit: {}", unit)),
    }
}

fn apply_datetime_adjustments(
    date: NaiveDate,
    time: NaiveTime,
    map: &HashMap<String, Value>,
    type_name: &str,
    tz_info: Option<&TimezoneInfo>,
) -> Result<Value> {
    // Apply date adjustments
    let year = map
        .get("year")
        .and_then(|v| v.as_i64())
        .unwrap_or(date.year() as i64) as i32;
    let month = map
        .get("month")
        .and_then(|v| v.as_i64())
        .unwrap_or(date.month() as i64) as u32;
    let day = map
        .get("day")
        .and_then(|v| v.as_i64())
        .unwrap_or(date.day() as i64) as u32;

    // Apply time adjustments
    let hour = map
        .get("hour")
        .and_then(|v| v.as_i64())
        .unwrap_or(time.hour() as i64) as u32;
    let minute = map
        .get("minute")
        .and_then(|v| v.as_i64())
        .unwrap_or(time.minute() as i64) as u32;
    let second = map
        .get("second")
        .and_then(|v| v.as_i64())
        .unwrap_or(time.second() as i64) as u32;
    let nanos = if map.contains_key("millisecond")
        || map.contains_key("microsecond")
        || map.contains_key("nanosecond")
    {
        build_nanoseconds(map)
    } else {
        time.nanosecond()
    };

    let adjusted_date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| anyhow!("Invalid date in adjustment"))?;
    let adjusted_time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)
        .ok_or_else(|| anyhow!("Invalid time in adjustment"))?;

    let ndt = NaiveDateTime::new(adjusted_date, adjusted_time);

    if type_name == "localdatetime" {
        Ok(Value::String(format_naive_datetime(&ndt)))
    } else if let Some(tz) = tz_info {
        let offset = tz.offset_for_local(&ndt)?;
        let dt = offset
            .from_local_datetime(&ndt)
            .single()
            .ok_or_else(|| anyhow!("Ambiguous local time"))?;
        Ok(Value::String(format_datetime_with_offset_and_tz(
            &dt,
            tz.name(),
        )))
    } else {
        let dt = DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc);
        Ok(Value::String(format_datetime_with_nanos(&dt)))
    }
}

// ============================================================================
// Duration Between Functions
// ============================================================================

/// Compute calendar months between two dates.
///
/// Returns the number of whole months from `start` to `end`.
/// Negative if `end` is before `start`.
fn calendar_months_between(start: &NaiveDate, end: &NaiveDate) -> i64 {
    let year_diff = end.year() as i64 - start.year() as i64;
    let month_diff = end.month() as i64 - start.month() as i64;
    let total_months = year_diff * 12 + month_diff;

    // Adjust if end day is before start day (incomplete month)
    if total_months > 0 && end.day() < start.day() {
        total_months - 1
    } else if total_months < 0 && end.day() > start.day() {
        total_months + 1
    } else {
        total_months
    }
}

/// Compute the remaining days after removing whole months.
fn remaining_days_after_months(start: &NaiveDate, end: &NaiveDate, months: i64) -> i64 {
    let after_months = add_months_to_date(*start, months);
    end.signed_duration_since(after_months).num_days()
}

fn eval_duration_between(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(anyhow!("duration.between requires two temporal arguments"));
    }
    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::Null);
    }

    let start = parse_temporal_value_typed(&args[0])?;
    let end = parse_temporal_value_typed(&args[1])?;

    let start_has_date = has_date_component(start.ttype);
    let end_has_date = has_date_component(end.ttype);
    let start_has_time = has_time_component(start.ttype);
    let end_has_time = has_time_component(end.ttype);

    // Both are date-only: return calendar months + remaining days, no time component.
    if start.ttype == TemporalType::Date && end.ttype == TemporalType::Date {
        let months = calendar_months_between(&start.local_date, &end.local_date);
        let remaining_days =
            remaining_days_after_months(&start.local_date, &end.local_date, months);
        let dur = CypherDuration::new(months, remaining_days, 0);
        return Ok(Value::String(dur.to_iso8601()));
    }

    // Both have date and time: calendar months + remaining time as nanos (no days).
    // Only use UTC normalization when BOTH operands have timezone info.
    if start_has_date && end_has_date && start_has_time && end_has_time {
        let both_tz_aware = start.utc_offset_secs.is_some() && end.utc_offset_secs.is_some();
        let (s_date, s_time, e_date, e_time) = if both_tz_aware {
            (
                start.utc_datetime.date(),
                start.utc_datetime.time(),
                end.utc_datetime.date(),
                end.utc_datetime.time(),
            )
        } else {
            (
                start.local_date,
                start.local_time,
                end.local_date,
                end.local_time,
            )
        };

        let months = calendar_months_between(&s_date, &e_date);
        let date_after_months = add_months_to_date(s_date, months);
        let start_dt = NaiveDateTime::new(date_after_months, s_time);
        let end_dt = NaiveDateTime::new(e_date, e_time);
        let remaining_nanos = end_dt
            .signed_duration_since(start_dt)
            .num_nanoseconds()
            .unwrap_or(0);

        let dur = CypherDuration::new(months, 0, remaining_nanos);
        return Ok(Value::String(dur.to_iso8601()));
    }

    // One has date+time, other is date-only: months + days + remaining time.
    if start_has_date && end_has_date {
        let both_tz_aware = start.utc_offset_secs.is_some() && end.utc_offset_secs.is_some();
        let (s_date, s_time, e_date, e_time) = if both_tz_aware {
            (
                start.utc_datetime.date(),
                start.utc_datetime.time(),
                end.utc_datetime.date(),
                end.utc_datetime.time(),
            )
        } else {
            (
                start.local_date,
                start.local_time,
                end.local_date,
                end.local_time,
            )
        };

        let months = calendar_months_between(&s_date, &e_date);
        let date_after_months = add_months_to_date(s_date, months);
        let start_dt = NaiveDateTime::new(date_after_months, s_time);
        let end_dt = NaiveDateTime::new(e_date, e_time);
        let remaining = end_dt.signed_duration_since(start_dt);
        let remaining_days = remaining.num_days();
        let remaining_nanos =
            remaining.num_nanoseconds().unwrap_or(0) - remaining_days * 86_400_000_000_000;

        let dur = CypherDuration::new(months, remaining_days, remaining_nanos);
        return Ok(Value::String(dur.to_iso8601()));
    }

    // Cross-type: one has date, other is time-only, or both time-only.
    // Use UTC normalization only when BOTH operands have timezone info.
    let both_tz_aware = start.utc_offset_secs.is_some() && end.utc_offset_secs.is_some();
    let start_time = if both_tz_aware {
        start.utc_datetime.time()
    } else {
        start.local_time
    };
    let end_time = if both_tz_aware {
        end.utc_datetime.time()
    } else {
        end.local_time
    };

    let start_nanos = time_to_nanos(&start_time);
    let end_nanos = time_to_nanos(&end_time);
    let nanos_diff = end_nanos - start_nanos;

    let dur = CypherDuration::new(0, 0, nanos_diff);
    Ok(Value::String(dur.to_iso8601()))
}

/// Check if a temporal type has a date component.
fn has_date_component(ttype: TemporalType) -> bool {
    matches!(
        ttype,
        TemporalType::Date | TemporalType::LocalDateTime | TemporalType::DateTime
    )
}

/// Check if a temporal type has a time component.
fn has_time_component(ttype: TemporalType) -> bool {
    matches!(
        ttype,
        TemporalType::LocalTime
            | TemporalType::Time
            | TemporalType::LocalDateTime
            | TemporalType::DateTime
    )
}

fn eval_duration_in_months(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(anyhow!("duration.inMonths requires two temporal arguments"));
    }
    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::Null);
    }

    let start = parse_temporal_value_typed(&args[0])?;
    let end = parse_temporal_value_typed(&args[1])?;

    if has_date_component(start.ttype) && has_date_component(end.ttype) {
        // Only use UTC normalization when both operands have timezone info
        let both_tz = start.utc_offset_secs.is_some() && end.utc_offset_secs.is_some();
        let (s_date, s_time, e_date, e_time) = if both_tz {
            (
                start.utc_datetime.date(),
                start.utc_datetime.time(),
                end.utc_datetime.date(),
                end.utc_datetime.time(),
            )
        } else {
            (
                start.local_date,
                start.local_time,
                end.local_date,
                end.local_time,
            )
        };
        let mut months = calendar_months_between(&s_date, &e_date);
        // Adjust months if the time component crosses the day boundary:
        // When both fall on the same day-of-month, time determines if we've
        // crossed the boundary. E.g., 2018-07-21T00:00 -> 2016-07-21T21:40
        // is only 23 months (not 24) because end time is later in the day.
        if s_date.day() == e_date.day() {
            if months > 0 && e_time < s_time {
                months -= 1;
            } else if months < 0 && e_time > s_time {
                months += 1;
            }
        }
        let dur = CypherDuration::new(months, 0, 0);
        Ok(Value::String(dur.to_iso8601()))
    } else {
        Ok(Value::String("PT0S".to_string()))
    }
}

fn eval_duration_in_days(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(anyhow!("duration.inDays requires two temporal arguments"));
    }
    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::Null);
    }

    let start = parse_temporal_value_typed(&args[0])?;
    let end = parse_temporal_value_typed(&args[1])?;

    if has_date_component(start.ttype) && has_date_component(end.ttype) {
        // Only use UTC normalization when both operands have timezone info.
        let both_tz = start.utc_offset_secs.is_some() && end.utc_offset_secs.is_some();
        let (s_dt, e_dt) = if both_tz {
            (start.utc_datetime, end.utc_datetime)
        } else {
            (
                NaiveDateTime::new(start.local_date, start.local_time),
                NaiveDateTime::new(end.local_date, end.local_time),
            )
        };
        // Compute total duration, then express as whole days (truncating toward zero).
        let total_nanos = e_dt
            .signed_duration_since(s_dt)
            .num_nanoseconds()
            .ok_or_else(|| anyhow!("Duration overflow in inDays"))?;
        let days = total_nanos / 86_400_000_000_000;
        let dur = CypherDuration::new(0, days, 0);
        Ok(Value::String(dur.to_iso8601()))
    } else {
        Ok(Value::String("PT0S".to_string()))
    }
}

/// Normalize a local datetime to UTC using a named IANA timezone.
///
/// When one operand has a named timezone (DST-aware) and the other is local
/// (no timezone), the local value must be interpreted in that named timezone
/// to correctly account for DST transitions.
fn normalize_local_to_utc(ndt: NaiveDateTime, tz: Tz) -> Result<NaiveDateTime> {
    use chrono::TimeZone;
    match tz.from_local_datetime(&ndt) {
        chrono::LocalResult::Single(dt) => Ok(dt.naive_utc()),
        chrono::LocalResult::Ambiguous(earliest, _) => Ok(earliest.naive_utc()),
        chrono::LocalResult::None => {
            // In a DST gap, shift forward by 1 hour and retry.
            let shifted = ndt + chrono::Duration::hours(1);
            match tz.from_local_datetime(&shifted) {
                chrono::LocalResult::Single(dt) => Ok(dt.naive_utc()),
                chrono::LocalResult::Ambiguous(earliest, _) => Ok(earliest.naive_utc()),
                _ => Err(anyhow!("Cannot resolve local time in timezone")),
            }
        }
    }
}

fn eval_duration_in_seconds(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(anyhow!(
            "duration.inSeconds requires two temporal arguments"
        ));
    }
    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::Null);
    }

    let start = parse_temporal_value_typed(&args[0])?;
    let end = parse_temporal_value_typed(&args[1])?;

    let start_has_date = has_date_component(start.ttype);
    let end_has_date = has_date_component(end.ttype);

    // Determine the shared named timezone for DST-aware normalization.
    // When one operand has a named (DST-aware) timezone (e.g., Europe/Stockholm),
    // local operands are interpreted in that timezone for correct DST handling.
    let shared_named_tz = start.named_tz.or(end.named_tz);

    // Resolve a temporal operand to a NaiveDateTime for comparison.
    //
    // Strategy:
    // - If a shared named timezone exists (DST scenario), normalize everything
    //   to UTC: tz-aware operands use their pre-computed UTC, local operands
    //   are interpreted in the shared named timezone then converted to UTC.
    // - If both operands have timezone info (fixed offsets), normalize to UTC.
    // - Otherwise (mixed local + fixed-offset, or both local), use face values.
    let both_have_tz = start.utc_offset_secs.is_some() && end.utc_offset_secs.is_some();

    let resolve =
        |pt: &ParsedTemporal, date_override: Option<NaiveDate>| -> Result<NaiveDateTime> {
            let local_date = date_override.unwrap_or(pt.local_date);
            let local_ndt = NaiveDateTime::new(local_date, pt.local_time);

            if let Some(tz) = shared_named_tz {
                // DST-aware mode: normalize everything to UTC.
                if pt.named_tz.is_some() && date_override.is_none() {
                    // This operand owns the named tz — already UTC-normalized.
                    Ok(pt.utc_datetime)
                } else {
                    // Local operand or date-overridden: interpret in the shared tz.
                    normalize_local_to_utc(local_ndt, tz)
                }
            } else if both_have_tz {
                // Both have fixed offsets: use UTC normalization.
                if date_override.is_some() {
                    let offset = pt.utc_offset_secs.unwrap_or(0);
                    Ok(local_ndt - chrono::Duration::seconds(offset as i64))
                } else {
                    Ok(pt.utc_datetime)
                }
            } else {
                // Mixed local + fixed-offset or both local: use face values.
                Ok(local_ndt)
            }
        };

    // Cross-type with time-only operand.
    if !start_has_date || !end_has_date {
        if shared_named_tz.is_some() {
            // DST mode: place time-only operand on the date-bearing operand's
            // local date within the shared timezone.
            let ref_date = if start_has_date {
                start.local_date
            } else if end_has_date {
                end.local_date
            } else {
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
            };
            let s_dt = resolve(&start, Some(ref_date))?;
            let e_dt = resolve(&end, Some(ref_date))?;
            let total_nanos = e_dt
                .signed_duration_since(s_dt)
                .num_nanoseconds()
                .ok_or_else(|| anyhow!("Duration overflow in inSeconds"))?;
            let dur = CypherDuration::new(0, 0, total_nanos);
            return Ok(Value::String(dur.to_iso8601()));
        }

        // No named timezone: simple time difference.
        let s_time = if both_have_tz {
            start.utc_datetime.time()
        } else {
            start.local_time
        };
        let e_time = if both_have_tz {
            end.utc_datetime.time()
        } else {
            end.local_time
        };
        let s_nanos = time_to_nanos(&s_time);
        let e_nanos = time_to_nanos(&e_time);
        let dur = CypherDuration::new(0, 0, e_nanos - s_nanos);
        return Ok(Value::String(dur.to_iso8601()));
    }

    // Both have date: use full datetime difference.
    let s_dt = resolve(&start, None)?;
    let e_dt = resolve(&end, None)?;
    let total_nanos = e_dt
        .signed_duration_since(s_dt)
        .num_nanoseconds()
        .ok_or_else(|| anyhow!("Duration overflow in inSeconds"))?;

    let dur = CypherDuration::new(0, 0, total_nanos);
    Ok(Value::String(dur.to_iso8601()))
}

/// Parsed temporal value with local and UTC-normalized components.
struct ParsedTemporal {
    /// Local date component (as written, before any UTC normalization).
    local_date: NaiveDate,
    /// Local time component (as written, before any UTC normalization).
    local_time: NaiveTime,
    /// UTC-normalized datetime (for absolute difference computation).
    utc_datetime: NaiveDateTime,
    /// Detected temporal type.
    ttype: TemporalType,
    /// Timezone offset in seconds from UTC, if applicable.
    utc_offset_secs: Option<i32>,
    /// The named IANA timezone, if present (for DST-aware cross-type computation).
    named_tz: Option<Tz>,
}

/// Parse a temporal value into local components, UTC-normalized datetime, and type.
fn parse_temporal_value_typed(val: &Value) -> Result<ParsedTemporal> {
    let midnight =
        NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| anyhow!("Failed to create midnight"))?;
    let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    match val {
        Value::String(s) => {
            let ttype = classify_temporal(s)
                .ok_or_else(|| anyhow!("Cannot classify temporal value: {}", s))?;

            match ttype {
                TemporalType::DateTime => {
                    let (date, time, tz_info) = parse_datetime_with_tz(s)?;
                    let local_ndt = NaiveDateTime::new(date, time);
                    let iana_tz = tz_info.as_ref().and_then(|info| match info {
                        TimezoneInfo::Named(tz) => Some(*tz),
                        _ => None,
                    });
                    let offset_secs = if let Some(ref info) = tz_info {
                        info.offset_for_local(&local_ndt)?.local_minus_utc()
                    } else {
                        0
                    };
                    let utc_ndt = local_ndt - chrono::Duration::seconds(offset_secs as i64);
                    Ok(ParsedTemporal {
                        local_date: date,
                        local_time: time,
                        utc_datetime: utc_ndt,
                        ttype,
                        utc_offset_secs: Some(offset_secs),

                        named_tz: iana_tz,
                    })
                }
                TemporalType::LocalDateTime => {
                    let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
                        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M"))
                        .map_err(|_| anyhow!("Cannot parse localdatetime: {}", s))?;
                    Ok(ParsedTemporal {
                        local_date: ndt.date(),
                        local_time: ndt.time(),
                        utc_datetime: ndt,
                        ttype,
                        utc_offset_secs: None,

                        named_tz: None,
                    })
                }
                TemporalType::Date => {
                    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .map_err(|_| anyhow!("Cannot parse date: {}", s))?;
                    let ndt = NaiveDateTime::new(d, midnight);
                    Ok(ParsedTemporal {
                        local_date: d,
                        local_time: midnight,
                        utc_datetime: ndt,
                        ttype,
                        utc_offset_secs: None,

                        named_tz: None,
                    })
                }
                TemporalType::Time => {
                    let (_, time, tz_info) = parse_datetime_with_tz(s)?;
                    let offset_secs = if let Some(ref info) = tz_info {
                        let dummy_ndt = NaiveDateTime::new(epoch_date, time);
                        info.offset_for_local(&dummy_ndt)?.local_minus_utc()
                    } else {
                        0
                    };
                    let local_ndt = NaiveDateTime::new(epoch_date, time);
                    let utc_ndt = local_ndt - chrono::Duration::seconds(offset_secs as i64);
                    Ok(ParsedTemporal {
                        local_date: epoch_date,
                        local_time: time,
                        utc_datetime: utc_ndt,
                        ttype,
                        utc_offset_secs: Some(offset_secs),

                        named_tz: None,
                    })
                }
                TemporalType::LocalTime => {
                    let time = parse_time_string(s)?;
                    let ndt = NaiveDateTime::new(epoch_date, time);
                    Ok(ParsedTemporal {
                        local_date: epoch_date,
                        local_time: time,
                        utc_datetime: ndt,
                        ttype,
                        utc_offset_secs: None,

                        named_tz: None,
                    })
                }
                TemporalType::Duration => Err(anyhow!("Cannot use duration as temporal argument")),
            }
        }
        _ => Err(anyhow!("Expected string temporal value")),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build a Value::Map from key-value pairs.
    fn map_val(pairs: Vec<(&str, Value)>) -> Value {
        Value::Map(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    #[test]
    fn test_date_from_map_calendar() {
        let result = eval_date(&[map_val(vec![
            ("year", Value::Int(1984)),
            ("month", Value::Int(10)),
            ("day", Value::Int(11)),
        ])])
        .unwrap();
        assert_eq!(result, Value::String("1984-10-11".to_string()));
    }

    #[test]
    fn test_date_from_map_defaults() {
        let result = eval_date(&[map_val(vec![("year", Value::Int(1984))])]).unwrap();
        assert_eq!(result, Value::String("1984-01-01".to_string()));
    }

    #[test]
    fn test_date_from_week() {
        // Week 10, Wednesday (day 3) of 1984
        let result = eval_date(&[map_val(vec![
            ("year", Value::Int(1984)),
            ("week", Value::Int(10)),
            ("dayOfWeek", Value::Int(3)),
        ])])
        .unwrap();
        assert!(result.as_str().unwrap().starts_with("1984-03"));
    }

    #[test]
    fn test_date_from_ordinal() {
        // Day 202 of 1984 (leap year)
        let result = eval_date(&[map_val(vec![
            ("year", Value::Int(1984)),
            ("ordinalDay", Value::Int(202)),
        ])])
        .unwrap();
        assert_eq!(result, Value::String("1984-07-20".to_string()));
    }

    #[test]
    fn test_date_from_quarter() {
        // Q3, day 45 of 1984
        let result = eval_date(&[map_val(vec![
            ("year", Value::Int(1984)),
            ("quarter", Value::Int(3)),
            ("dayOfQuarter", Value::Int(45)),
        ])])
        .unwrap();
        assert_eq!(result, Value::String("1984-08-14".to_string()));
    }

    #[test]
    fn test_time_from_map() {
        let result = eval_time(&[map_val(vec![
            ("hour", Value::Int(12)),
            ("minute", Value::Int(31)),
            ("second", Value::Int(14)),
        ])])
        .unwrap();
        assert_eq!(result, Value::String("12:31:14".to_string()));
    }

    #[test]
    fn test_time_from_map_with_nanos() {
        let result = eval_time(&[map_val(vec![
            ("hour", Value::Int(12)),
            ("minute", Value::Int(31)),
            ("second", Value::Int(14)),
            ("millisecond", Value::Int(645)),
            ("microsecond", Value::Int(876)),
            ("nanosecond", Value::Int(123)),
        ])])
        .unwrap();
        assert!(result.as_str().unwrap().starts_with("12:31:14.645876123"));
    }

    #[test]
    fn test_datetime_from_map() {
        let result = eval_datetime(&[map_val(vec![
            ("year", Value::Int(1984)),
            ("month", Value::Int(10)),
            ("day", Value::Int(11)),
            ("hour", Value::Int(12)),
        ])])
        .unwrap();
        assert!(result.as_str().unwrap().contains("1984-10-11T12:00"));
    }

    #[test]
    fn test_localdatetime_from_week() {
        // Week 1 of 1816 should be 1816-01-01 (Monday of that week)
        let result = eval_localdatetime(&[map_val(vec![
            ("year", Value::Int(1816)),
            ("week", Value::Int(1)),
        ])])
        .unwrap();
        assert_eq!(result, Value::String("1816-01-01T00:00".to_string()));

        // Week 52 of 1816
        let result = eval_localdatetime(&[map_val(vec![
            ("year", Value::Int(1816)),
            ("week", Value::Int(52)),
        ])])
        .unwrap();
        assert_eq!(result, Value::String("1816-12-23T00:00".to_string()));

        // Week 1 of 1817 (starts in 1816!)
        let result = eval_localdatetime(&[map_val(vec![
            ("year", Value::Int(1817)),
            ("week", Value::Int(1)),
        ])])
        .unwrap();
        assert_eq!(result, Value::String("1816-12-30T00:00".to_string()));
    }

    #[test]
    fn test_duration_from_map_extended() {
        let result = eval_duration(&[map_val(vec![
            ("years", Value::Int(1)),
            ("months", Value::Int(2)),
            ("days", Value::Int(3)),
        ])])
        .unwrap();
        // Duration is now returned as ISO 8601 string
        let dur_str = result.as_str().unwrap();
        assert!(dur_str.starts_with('P'));
        assert!(dur_str.contains('Y')); // Should have years (14 months = 1 year + 2 months)
        assert!(dur_str.contains('D')); // Should have days
    }

    #[test]
    fn test_datetime_fromepoch() {
        let result = eval_datetime_fromepoch(&[Value::Int(0)]).unwrap();
        assert_eq!(result, Value::String("1970-01-01T00:00Z".to_string()));
    }

    #[test]
    fn test_datetime_fromepochmillis() {
        let result = eval_datetime_fromepochmillis(&[Value::Int(0)]).unwrap();
        assert_eq!(result, Value::String("1970-01-01T00:00Z".to_string()));
    }

    #[test]
    fn test_truncate_date_year() {
        let result = eval_truncate(
            "date",
            &[
                Value::String("year".to_string()),
                Value::String("1984-10-11".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::String("1984-01-01".to_string()));
    }

    #[test]
    fn test_truncate_date_month() {
        let result = eval_truncate(
            "date",
            &[
                Value::String("month".to_string()),
                Value::String("1984-10-11".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::String("1984-10-01".to_string()));
    }

    #[test]
    fn test_truncate_datetime_hour() {
        let result = eval_truncate(
            "datetime",
            &[
                Value::String("hour".to_string()),
                Value::String("1984-10-11T12:31:14Z".to_string()),
            ],
        )
        .unwrap();
        assert!(result.as_str().unwrap().contains("1984-10-11T12:00"));
    }

    #[test]
    fn test_duration_between() {
        let result = eval_duration_between(&[
            Value::String("1984-10-11".to_string()),
            Value::String("1984-10-12".to_string()),
        ])
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "P1D");
    }

    #[test]
    fn test_duration_in_days() {
        let result = eval_duration_in_days(&[
            Value::String("1984-10-11".to_string()),
            Value::String("1984-10-21".to_string()),
        ])
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "P10D");
    }

    #[test]
    fn test_duration_in_months() {
        let result = eval_duration_in_months(&[
            Value::String("1984-10-11".to_string()),
            Value::String("1985-01-11".to_string()),
        ])
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "P3M");
    }

    #[test]
    fn test_duration_in_seconds() {
        let result = eval_duration_in_seconds(&[
            Value::String("1984-10-11T12:00:00".to_string()),
            Value::String("1984-10-11T13:00:00".to_string()),
        ])
        .unwrap();
        assert_eq!(result.as_str().unwrap(), "PT1H");
    }

    #[test]
    fn test_classify_temporal() {
        assert_eq!(classify_temporal("1984-10-11"), Some(TemporalType::Date));
        assert_eq!(classify_temporal("12:31:14"), Some(TemporalType::LocalTime));
        assert_eq!(
            classify_temporal("12:31:14+01:00"),
            Some(TemporalType::Time)
        );
        assert_eq!(
            classify_temporal("1984-10-11T12:31:14"),
            Some(TemporalType::LocalDateTime)
        );
        assert_eq!(
            classify_temporal("1984-10-11T12:31:14Z"),
            Some(TemporalType::DateTime)
        );
        assert_eq!(
            classify_temporal("1984-10-11T12:31:14+01:00"),
            Some(TemporalType::DateTime)
        );
        assert_eq!(classify_temporal("P1Y2M3D"), Some(TemporalType::Duration));
    }

    #[test]
    fn test_add_months_to_date_clamping() {
        // Jan 31 + 1 month = Feb 28 (non-leap year)
        let date = NaiveDate::from_ymd_opt(2023, 1, 31).unwrap();
        let result = add_months_to_date(date, 1);
        assert_eq!(result, NaiveDate::from_ymd_opt(2023, 2, 28).unwrap());

        // Jan 31 + 1 month in leap year = Feb 29
        let date = NaiveDate::from_ymd_opt(2024, 1, 31).unwrap();
        let result = add_months_to_date(date, 1);
        assert_eq!(result, NaiveDate::from_ymd_opt(2024, 2, 29).unwrap());
    }

    #[test]
    fn test_cypher_duration_multiply() {
        let dur = CypherDuration::new(1, 1, 0);
        let result = dur.multiply(2.0);
        assert_eq!(result.months, 2);
        assert_eq!(result.days, 2);
    }

    #[test]
    fn test_fractional_cascading_in_map() {
        // months: 5.5 cascades via avg Gregorian month (2629746s).
        // 0.5 months = 1314873s = 15 days + 18873s = 15d 5h 14m 33s
        let result = eval_duration(&[map_val(vec![
            ("months", Value::Float(5.5)),
            ("days", Value::Int(0)),
        ])])
        .unwrap();
        let s = result.as_str().unwrap();
        assert_eq!(s, "P5M15DT5H14M33S");
    }

    #[test]
    fn test_fractional_cascading_full() {
        let result = eval_duration(&[map_val(vec![
            ("years", Value::Float(12.5)),
            ("months", Value::Float(5.5)),
            ("days", Value::Float(14.5)),
            ("hours", Value::Float(16.5)),
            ("minutes", Value::Float(12.5)),
            ("seconds", Value::Float(70.5)),
            ("nanoseconds", Value::Int(3)),
        ])])
        .unwrap();
        let s = result.as_str().unwrap();
        // Verify roundtrip
        let dur = parse_duration_to_cypher(s).unwrap();
        assert_eq!(dur.months, 155);
        assert_eq!(dur.days, 29);
    }

    #[test]
    fn test_parse_iso8601_duration_with_weeks() {
        let micros = parse_duration_to_micros("P1W").unwrap();
        assert_eq!(micros, 7 * MICROS_PER_DAY);
    }

    #[test]
    fn test_parse_iso8601_duration_complex() {
        let micros = parse_duration_to_micros("P1DT2H30M").unwrap();
        let expected = MICROS_PER_DAY + 2 * MICROS_PER_HOUR + 30 * MICROS_PER_MINUTE;
        assert_eq!(micros, expected);
    }
}
