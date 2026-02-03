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
use serde_json::{Map, Value, json};

// ============================================================================
// Constants
// ============================================================================

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
const NANOS_PER_SECOND: i64 = 1_000_000_000;

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
    fn offset_for_local(&self, ndt: &NaiveDateTime) -> Result<FixedOffset> {
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
    is_duration_value(val) || matches!(val, Value::Number(n) if n.is_i64())
}

/// Convert a duration value (ISO 8601 string or i64 micros) to microseconds.
pub fn duration_to_micros(val: &Value) -> Result<i64> {
    match val {
        Value::String(s) => {
            let duration = parse_duration_to_cypher(s)?;
            Ok(duration.to_micros())
        }
        Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| anyhow!("Expected integer duration")),
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
                return Ok(json!(extract_component(&dt, &component)));
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                return Ok(json!(extract_component(&dt, &component)));
            }

            match component {
                Component::Year | Component::Month | Component::Day => {
                    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        return Ok(json!(match component {
                            Component::Year => d.year(),
                            Component::Month => d.month() as i32,
                            Component::Day => d.day() as i32,
                            _ => unreachable!(),
                        }));
                    }
                }
                Component::Hour | Component::Minute | Component::Second => {
                    if let Ok(t) = NaiveTime::parse_from_str(s, "%H:%M:%S") {
                        return Ok(json!(match component {
                            Component::Hour => t.hour() as i32,
                            Component::Minute => t.minute() as i32,
                            Component::Second => t.second() as i32,
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
    // Quick checks for common patterns
    if s.len() < 8 {
        return false;
    }

    // Date pattern: YYYY-MM-DD
    if s.len() >= 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
        return true;
    }

    // Time pattern: HH:MM:SS
    if s.len() >= 8 && s.chars().nth(2) == Some(':') && s.chars().nth(5) == Some(':') {
        return true;
    }

    // Duration pattern: starts with P
    if s.starts_with('P') || s.starts_with('p') {
        return true;
    }

    false
}

/// Check if a string looks like a duration value.
pub fn is_duration_string(s: &str) -> bool {
    s.starts_with('P') || s.starts_with('p')
}

// Individual component extractors

fn extract_year(s: &str) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    Ok(json!(date.year()))
}

fn extract_month(s: &str) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    Ok(json!(date.month() as i64))
}

fn extract_day(s: &str) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    Ok(json!(date.day() as i64))
}

fn extract_hour(s: &str) -> Result<Value> {
    let (_, time, _) = parse_datetime_with_tz(s)?;
    Ok(json!(time.hour() as i64))
}

fn extract_minute(s: &str) -> Result<Value> {
    let (_, time, _) = parse_datetime_with_tz(s)?;
    Ok(json!(time.minute() as i64))
}

fn extract_second(s: &str) -> Result<Value> {
    let (_, time, _) = parse_datetime_with_tz(s)?;
    Ok(json!(time.second() as i64))
}

fn extract_quarter(s: &str) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    let quarter = (date.month() - 1) / 3 + 1;
    Ok(json!(quarter as i64))
}

fn extract_week(s: &str) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    let week = date.iso_week().week();
    Ok(json!(week as i64))
}

fn extract_week_year(s: &str) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    let week_year = date.iso_week().year();
    Ok(json!(week_year))
}

fn extract_ordinal_day(s: &str) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    Ok(json!(date.ordinal() as i64))
}

fn extract_day_of_week(s: &str) -> Result<Value> {
    let (date, _, _) = parse_datetime_with_tz(s)?;
    // ISO weekday: Monday = 1, Sunday = 7
    let dow = date.weekday().num_days_from_monday() + 1;
    Ok(json!(dow as i64))
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
    Ok(json!(day_of_quarter))
}

fn extract_millisecond(s: &str) -> Result<Value> {
    let (_, time, _) = parse_datetime_with_tz(s)?;
    let millis = time.nanosecond() / 1_000_000;
    Ok(json!(millis as i64))
}

fn extract_microsecond(s: &str) -> Result<Value> {
    let (_, time, _) = parse_datetime_with_tz(s)?;
    let micros = time.nanosecond() / 1_000;
    Ok(json!(micros as i64))
}

fn extract_nanosecond(s: &str) -> Result<Value> {
    let (_, time, _) = parse_datetime_with_tz(s)?;
    Ok(json!(time.nanosecond() as i64))
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
    let (_, _, tz_info) = parse_datetime_with_tz(s)?;
    match tz_info {
        Some(ref tz) => {
            // Need to get offset for the actual datetime
            let (date, time, _) = parse_datetime_with_tz(s)?;
            let ndt = NaiveDateTime::new(date, time);
            let offset = tz.offset_for_local(&ndt)?;
            let secs = offset.local_minus_utc();
            Ok(Value::String(format_timezone_offset(secs)))
        }
        None => Ok(Value::Null),
    }
}

fn extract_offset_minutes(s: &str) -> Result<Value> {
    let (date, time, tz_info) = parse_datetime_with_tz(s)?;
    match tz_info {
        Some(ref tz) => {
            let ndt = NaiveDateTime::new(date, time);
            let offset = tz.offset_for_local(&ndt)?;
            let secs = offset.local_minus_utc();
            Ok(json!(secs / 60))
        }
        None => Ok(json!(0)),
    }
}

fn extract_offset_seconds(s: &str) -> Result<Value> {
    let (date, time, tz_info) = parse_datetime_with_tz(s)?;
    match tz_info {
        Some(ref tz) => {
            let ndt = NaiveDateTime::new(date, time);
            let offset = tz.offset_for_local(&ndt)?;
            Ok(json!(offset.local_minus_utc()))
        }
        None => Ok(json!(0)),
    }
}

fn extract_epoch_seconds(s: &str) -> Result<Value> {
    let (date, time, _) = parse_datetime_with_tz(s)?;
    let ndt = NaiveDateTime::new(date, time);
    let dt = DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc);
    Ok(json!(dt.timestamp()))
}

fn extract_epoch_millis(s: &str) -> Result<Value> {
    let (date, time, _) = parse_datetime_with_tz(s)?;
    let ndt = NaiveDateTime::new(date, time);
    let dt = DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc);
    Ok(json!(dt.timestamp_millis()))
}

// ============================================================================
// Duration Component Accessors
// ============================================================================

/// Evaluate a duration component accessor.
pub fn eval_duration_accessor(duration_str: &str, component: &str) -> Result<Value> {
    let duration = parse_duration_to_cypher(duration_str)?;
    let component_lower = component.to_lowercase();

    // Get values from duration components
    let total_months = duration.months;
    let total_nanos = duration.nanos;
    let total_secs = total_nanos / NANOS_PER_SECOND;

    match component_lower.as_str() {
        // Total components (converted to that unit)
        "years" => Ok(json!(total_months / 12)),
        "months" => Ok(json!(total_months)),
        "weeks" => Ok(json!(duration.days / 7)),
        "days" => Ok(json!(duration.days)),
        "hours" => Ok(json!(total_secs / 3600)),
        "minutes" => Ok(json!(total_secs / 60)),
        "seconds" => Ok(json!(total_secs)),
        "milliseconds" => Ok(json!(total_nanos / 1_000_000)),
        "microseconds" => Ok(json!(total_nanos / 1_000)),
        "nanoseconds" => Ok(json!(total_nanos)),

        // "Of" accessors (remainder within larger unit)
        "quartersofyear" => Ok(json!((total_months % 12) / 3)),
        "monthsofquarter" => Ok(json!(total_months % 3)),
        "monthsofyear" => Ok(json!(total_months % 12)),
        "daysofweek" => Ok(json!(duration.days % 7)),
        "hoursofday" => Ok(json!((total_secs / 3600) % 24)),
        "minutesofhour" => Ok(json!((total_secs / 60) % 60)),
        "secondsofminute" => Ok(json!(total_secs % 60)),
        "millisecondsofsecond" => Ok(json!((total_nanos / 1_000_000) % 1000)),
        "microsecondsofsecond" => Ok(json!((total_nanos / 1_000) % 1_000_000)),
        "nanosecondsofsecond" => Ok(json!(total_nanos % NANOS_PER_SECOND)),

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
        Value::Object(map) => eval_date_from_map(map),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("date() expects a string or map argument")),
    }
}

fn eval_date_from_map(map: &Map<String, Value>) -> Result<Value> {
    // Check if we have a 'date' field to copy from another date/datetime
    if let Some(dt_val) = map.get("date") {
        return eval_date_from_projection(map, dt_val);
    }

    let date = build_date_from_map(map)?;
    Ok(Value::String(date.format("%Y-%m-%d").to_string()))
}

/// Handle date construction from projection (copying from another temporal value).
fn eval_date_from_projection(map: &Map<String, Value>, source: &Value) -> Result<Value> {
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
    map: &Map<String, Value>,
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
fn build_date_from_map(map: &Map<String, Value>) -> Result<NaiveDate> {
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
        let now = Utc::now().time();
        return Ok(Value::String(format_time_with_nanos(&now)));
    }

    match &args[0] {
        Value::String(s) => {
            let time = parse_time_string(s)?;
            Ok(Value::String(format_time_with_nanos(&time)))
        }
        Value::Object(map) => eval_time_from_map(map, true),
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
        Value::Object(map) => eval_time_from_map(map, false),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("localtime() expects a string or map argument")),
    }
}

fn eval_time_from_map(map: &Map<String, Value>, with_timezone: bool) -> Result<Value> {
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
    map: &Map<String, Value>,
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
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| dt.time()))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").map(|dt| dt.time()))
        .or_else(|_| DateTime::parse_from_rfc3339(s).map(|dt| dt.time()))
        .map_err(|_| anyhow!("Invalid time format"))
}

fn build_nanoseconds(map: &Map<String, Value>) -> u32 {
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
            let dt: DateTime<Utc> = parse_datetime_utc(s)?;
            Ok(Value::String(format_datetime_with_nanos(&dt)))
        }
        Value::Object(map) => eval_datetime_from_map(map, true),
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
            // Parse and return as local datetime
            let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                .or_else(|_| DateTime::parse_from_rfc3339(s).map(|dt| dt.naive_local()))
                .map_err(|_| anyhow!("Invalid localdatetime format"))?;
            Ok(Value::String(format_naive_datetime(&ndt)))
        }
        Value::Object(map) => eval_datetime_from_map(map, false),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("localdatetime() expects a string or map argument")),
    }
}

fn eval_datetime_from_map(map: &Map<String, Value>, with_timezone: bool) -> Result<Value> {
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
    map: &Map<String, Value>,
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

/// Parse a datetime string and extract date, time, and timezone info.
fn parse_datetime_with_tz(s: &str) -> Result<(NaiveDate, NaiveTime, Option<TimezoneInfo>)> {
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
    if let Ok(ndt) = NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%dT%H:%M:%S") {
        let tz_info = tz_name.map(|n| parse_timezone(&n)).transpose()?;
        return Ok((ndt.date(), ndt.time(), tz_info));
    }

    if let Ok(ndt) = NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%dT%H:%M:%S%.f") {
        let tz_info = tz_name.map(|n| parse_timezone(&n)).transpose()?;
        return Ok((ndt.date(), ndt.time(), tz_info));
    }

    if let Ok(ndt) = NaiveDateTime::parse_from_str(datetime_part, "%Y-%m-%dT%H:%M") {
        let tz_info = tz_name.map(|n| parse_timezone(&n)).transpose()?;
        return Ok((ndt.date(), ndt.time(), tz_info));
    }

    // Date only
    if let Ok(d) = NaiveDate::parse_from_str(datetime_part, "%Y-%m-%d") {
        let tz_info = tz_name.map(|n| parse_timezone(&n)).transpose()?;
        return Ok((d, midnight, tz_info));
    }

    // Try parsing as time with timezone (e.g., "12:31:14.645876123+01:00")
    // First extract the timezone offset from the end
    if let Some(tz_pos) = datetime_part.rfind('+').or_else(|| {
        // Find the last '-' that's part of timezone, not time
        datetime_part.rfind('-').filter(|&pos| pos > 2)
    }) {
        let time_part = &datetime_part[..tz_pos];
        let tz_part = &datetime_part[tz_pos..];

        // Try parsing the time part
        if let Ok(time) = NaiveTime::parse_from_str(time_part, "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(time_part, "%H:%M:%S"))
            .or_else(|_| NaiveTime::parse_from_str(time_part, "%H:%M"))
        {
            let tz_info = if let Some(name) = tz_name {
                Some(parse_timezone(&name)?)
            } else {
                let offset = parse_timezone_offset(tz_part)?;
                let fo = FixedOffset::east_opt(offset)
                    .ok_or_else(|| anyhow!("Invalid timezone offset"))?;
                Some(TimezoneInfo::FixedOffset(fo))
            };
            return Ok((today, time, tz_info));
        }
    }

    // Try parsing time with Z suffix
    if let Some(time_part) = datetime_part.strip_suffix('Z')
        && let Ok(time) = NaiveTime::parse_from_str(time_part, "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(time_part, "%H:%M:%S"))
            .or_else(|_| NaiveTime::parse_from_str(time_part, "%H:%M"))
    {
        let tz_info = Some(TimezoneInfo::FixedOffset(FixedOffset::east_opt(0).unwrap()));
        return Ok((today, time, tz_info));
    }

    Err(anyhow!("Cannot parse datetime: {}", s))
}

fn format_datetime_with_nanos(dt: &DateTime<Utc>) -> String {
    let nanos = dt.nanosecond();
    if nanos == 0 {
        dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    } else if nanos.is_multiple_of(1_000_000) {
        dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    } else if nanos.is_multiple_of(1_000) {
        dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
    } else {
        dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
    }
}

fn format_datetime_with_offset_and_tz(dt: &DateTime<FixedOffset>, tz_name: Option<&str>) -> String {
    let nanos = dt.nanosecond();
    let base = if nanos == 0 {
        dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
    } else if nanos.is_multiple_of(1_000_000) {
        dt.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string()
    } else if nanos.is_multiple_of(1_000) {
        dt.format("%Y-%m-%dT%H:%M:%S%.6f%:z").to_string()
    } else {
        dt.format("%Y-%m-%dT%H:%M:%S%.9f%:z").to_string()
    };

    // Append timezone name if present (e.g., [Europe/Stockholm])
    if let Some(name) = tz_name {
        format!("{}[{}]", base, name)
    } else {
        base
    }
}

fn format_datetime_local(dt: &DateTime<chrono::Local>) -> String {
    let nanos = dt.nanosecond();
    if nanos == 0 {
        dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
    } else if nanos.is_multiple_of(1_000_000) {
        dt.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string()
    } else if nanos.is_multiple_of(1_000) {
        dt.format("%Y-%m-%dT%H:%M:%S%.6f%:z").to_string()
    } else {
        dt.format("%Y-%m-%dT%H:%M:%S%.9f%:z").to_string()
    }
}

fn format_naive_datetime(ndt: &NaiveDateTime) -> String {
    let nanos = ndt.nanosecond();
    let seconds = ndt.second();

    if nanos == 0 && seconds == 0 {
        // Omit seconds when zero
        ndt.format("%Y-%m-%dT%H:%M").to_string()
    } else if nanos == 0 {
        ndt.format("%Y-%m-%dT%H:%M:%S").to_string()
    } else if nanos.is_multiple_of(1_000_000) {
        ndt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
    } else if nanos.is_multiple_of(1_000) {
        ndt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
    } else {
        ndt.format("%Y-%m-%dT%H:%M:%S%.9f").to_string()
    }
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

        // Time part
        let total_secs = self.nanos / NANOS_PER_SECOND;
        let remaining_nanos = self.nanos % NANOS_PER_SECOND;

        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let seconds = total_secs % 60;

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
                    // Format with nanosecond precision
                    let secs_with_nanos = seconds as f64 + (remaining_nanos as f64 / 1e9);
                    // Remove trailing zeros
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
        Value::Object(map) => eval_duration_from_map(map),
        Value::Number(n) => {
            // Treat as microseconds, convert to ISO 8601
            if let Some(micros) = n.as_i64() {
                let duration = CypherDuration::from_micros(micros);
                Ok(Value::String(duration.to_iso8601()))
            } else {
                Ok(Value::Number(n.clone()))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("duration() expects a string, map, or number")),
    }
}

fn eval_duration_from_map(map: &Map<String, Value>) -> Result<Value> {
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut nanos: i64 = 0;

    // Calendar components
    if let Some(years) = map.get("years").and_then(get_numeric_value) {
        months += (years * 12.0) as i64;
    }
    if let Some(m) = map.get("months").and_then(get_numeric_value) {
        months += m as i64;
    }
    if let Some(weeks) = map.get("weeks").and_then(get_numeric_value) {
        days += (weeks * 7.0) as i64;
    }
    if let Some(d) = map.get("days").and_then(get_numeric_value) {
        days += d as i64;
    }

    // Time components (stored as nanoseconds)
    if let Some(hours) = map.get("hours").and_then(get_numeric_value) {
        nanos += (hours * 3600.0 * NANOS_PER_SECOND as f64) as i64;
    }
    if let Some(minutes) = map.get("minutes").and_then(get_numeric_value) {
        nanos += (minutes * 60.0 * NANOS_PER_SECOND as f64) as i64;
    }
    if let Some(seconds) = map.get("seconds").and_then(get_numeric_value) {
        nanos += (seconds * NANOS_PER_SECOND as f64) as i64;
    }
    if let Some(millis) = map.get("milliseconds").and_then(get_numeric_value) {
        nanos += (millis * 1_000_000.0) as i64;
    }
    if let Some(micros) = map.get("microseconds").and_then(get_numeric_value) {
        nanos += (micros * 1_000.0) as i64;
    }
    if let Some(n) = map.get("nanoseconds").and_then(get_numeric_value) {
        nanos += n as i64;
    }

    let duration = CypherDuration::new(months, days, nanos);
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

    // Format with millisecond precision
    let nanos = dt.nanosecond();
    let result = if nanos == 0 {
        dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    } else {
        dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    };
    Ok(Value::String(result))
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
    adjust_map: Option<&Map<String, Value>>,
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

fn apply_date_adjustments(date: NaiveDate, map: &Map<String, Value>) -> Result<Value> {
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
    adjust_map: Option<&Map<String, Value>>,
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
fn apply_time_adjustments(time: NaiveTime, map: &Map<String, Value>) -> Result<NaiveTime> {
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
    adjust_map: Option<&Map<String, Value>>,
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
    map: &Map<String, Value>,
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

fn eval_duration_between(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(anyhow!("duration.between requires two temporal arguments"));
    }

    let (start_date, start_time) = parse_temporal_value(&args[0])?;
    let (end_date, end_time) = parse_temporal_value(&args[1])?;

    let start_dt = NaiveDateTime::new(start_date, start_time);
    let end_dt = NaiveDateTime::new(end_date, end_time);

    let duration = end_dt.signed_duration_since(start_dt);
    let micros = duration
        .num_microseconds()
        .ok_or_else(|| anyhow!("Duration overflow"))?;

    Ok(json!(micros))
}

fn eval_duration_in_months(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(anyhow!("duration.inMonths requires two temporal arguments"));
    }

    let (start_date, _) = parse_temporal_value(&args[0])?;
    let (end_date, _) = parse_temporal_value(&args[1])?;

    // Calculate whole months between dates
    let year_diff = end_date.year() - start_date.year();
    let month_diff = end_date.month() as i32 - start_date.month() as i32;
    let total_months = year_diff * 12 + month_diff;

    // Adjust if end day is before start day (incomplete month)
    let adjusted_months = if end_date.day() < start_date.day() {
        total_months - 1
    } else {
        total_months
    };

    // Return as duration in microseconds (approximate: 30 days per month)
    let micros = adjusted_months as i64 * 30 * MICROS_PER_DAY;
    Ok(json!(micros))
}

fn eval_duration_in_days(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(anyhow!("duration.inDays requires two temporal arguments"));
    }

    let (start_date, _) = parse_temporal_value(&args[0])?;
    let (end_date, _) = parse_temporal_value(&args[1])?;

    let days = end_date.signed_duration_since(start_date).num_days();
    let micros = days * MICROS_PER_DAY;

    Ok(json!(micros))
}

fn eval_duration_in_seconds(args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Err(anyhow!(
            "duration.inSeconds requires two temporal arguments"
        ));
    }

    let (start_date, start_time) = parse_temporal_value(&args[0])?;
    let (end_date, end_time) = parse_temporal_value(&args[1])?;

    let start_dt = NaiveDateTime::new(start_date, start_time);
    let end_dt = NaiveDateTime::new(end_date, end_time);

    let duration = end_dt.signed_duration_since(start_dt);
    let micros = duration
        .num_microseconds()
        .ok_or_else(|| anyhow!("Duration overflow"))?;

    Ok(json!(micros))
}

fn parse_temporal_value(val: &Value) -> Result<(NaiveDate, NaiveTime)> {
    let midnight =
        NaiveTime::from_hms_opt(0, 0, 0).ok_or_else(|| anyhow!("Failed to create midnight"))?;

    match val {
        Value::String(s) => {
            // Try datetime formats first
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                return Ok((dt.date_naive(), dt.time()));
            }
            if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                return Ok((ndt.date(), ndt.time()));
            }
            if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
                return Ok((ndt.date(), ndt.time()));
            }
            // Try date only
            if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                return Ok((d, midnight));
            }
            // Try time only (use epoch date)
            if let Ok(t) = parse_time_string(s) {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .ok_or_else(|| anyhow!("Failed to create epoch date"))?;
                return Ok((epoch, t));
            }
            Err(anyhow!("Cannot parse temporal value: {}", s))
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
    use serde_json::json;

    #[test]
    fn test_date_from_map_calendar() {
        let result = eval_date(&[json!({"year": 1984, "month": 10, "day": 11})]).unwrap();
        assert_eq!(result, Value::String("1984-10-11".to_string()));
    }

    #[test]
    fn test_date_from_map_defaults() {
        let result = eval_date(&[json!({"year": 1984})]).unwrap();
        assert_eq!(result, Value::String("1984-01-01".to_string()));
    }

    #[test]
    fn test_date_from_week() {
        // Week 10, Wednesday (day 3) of 1984
        let result = eval_date(&[json!({"year": 1984, "week": 10, "dayOfWeek": 3})]).unwrap();
        assert!(result.as_str().unwrap().starts_with("1984-03"));
    }

    #[test]
    fn test_date_from_ordinal() {
        // Day 202 of 1984 (leap year)
        let result = eval_date(&[json!({"year": 1984, "ordinalDay": 202})]).unwrap();
        assert_eq!(result, Value::String("1984-07-20".to_string()));
    }

    #[test]
    fn test_date_from_quarter() {
        // Q3, day 45 of 1984
        let result = eval_date(&[json!({"year": 1984, "quarter": 3, "dayOfQuarter": 45})]).unwrap();
        assert_eq!(result, Value::String("1984-08-14".to_string()));
    }

    #[test]
    fn test_time_from_map() {
        let result = eval_time(&[json!({"hour": 12, "minute": 31, "second": 14})]).unwrap();
        assert_eq!(result, Value::String("12:31:14".to_string()));
    }

    #[test]
    fn test_time_from_map_with_nanos() {
        let result = eval_time(&[json!({
            "hour": 12,
            "minute": 31,
            "second": 14,
            "millisecond": 645,
            "microsecond": 876,
            "nanosecond": 123
        })])
        .unwrap();
        assert!(result.as_str().unwrap().starts_with("12:31:14.645876123"));
    }

    #[test]
    fn test_datetime_from_map() {
        let result =
            eval_datetime(&[json!({"year": 1984, "month": 10, "day": 11, "hour": 12})]).unwrap();
        assert!(result.as_str().unwrap().contains("1984-10-11T12:00:00"));
    }

    #[test]
    fn test_localdatetime_from_week() {
        // Week 1 of 1816 should be 1816-01-01 (Monday of that week)
        let result = eval_localdatetime(&[json!({"year": 1816, "week": 1})]).unwrap();
        assert_eq!(result, Value::String("1816-01-01T00:00".to_string()));

        // Week 52 of 1816
        let result = eval_localdatetime(&[json!({"year": 1816, "week": 52})]).unwrap();
        assert_eq!(result, Value::String("1816-12-23T00:00".to_string()));

        // Week 1 of 1817 (starts in 1816!)
        let result = eval_localdatetime(&[json!({"year": 1817, "week": 1})]).unwrap();
        assert_eq!(result, Value::String("1816-12-30T00:00".to_string()));
    }

    #[test]
    fn test_duration_from_map_extended() {
        let result = eval_duration(&[json!({"years": 1, "months": 2, "days": 3})]).unwrap();
        // Duration is now returned as ISO 8601 string
        let dur_str = result.as_str().unwrap();
        assert!(dur_str.starts_with('P'));
        assert!(dur_str.contains('Y')); // Should have years (14 months = 1 year + 2 months)
        assert!(dur_str.contains('D')); // Should have days
    }

    #[test]
    fn test_datetime_fromepoch() {
        let result = eval_datetime_fromepoch(&[json!(0)]).unwrap();
        assert_eq!(result, Value::String("1970-01-01T00:00:00Z".to_string()));
    }

    #[test]
    fn test_datetime_fromepochmillis() {
        let result = eval_datetime_fromepochmillis(&[json!(0)]).unwrap();
        assert_eq!(result, Value::String("1970-01-01T00:00:00Z".to_string()));
    }

    #[test]
    fn test_truncate_date_year() {
        let result = eval_truncate("date", &[json!("year"), json!("1984-10-11")]).unwrap();
        assert_eq!(result, Value::String("1984-01-01".to_string()));
    }

    #[test]
    fn test_truncate_date_month() {
        let result = eval_truncate("date", &[json!("month"), json!("1984-10-11")]).unwrap();
        assert_eq!(result, Value::String("1984-10-01".to_string()));
    }

    #[test]
    fn test_truncate_datetime_hour() {
        let result =
            eval_truncate("datetime", &[json!("hour"), json!("1984-10-11T12:31:14Z")]).unwrap();
        assert!(result.as_str().unwrap().contains("1984-10-11T12:00:00"));
    }

    #[test]
    fn test_duration_between() {
        let result = eval_duration_between(&[json!("1984-10-11"), json!("1984-10-12")]).unwrap();
        let micros = result.as_i64().unwrap();
        assert_eq!(micros, MICROS_PER_DAY);
    }

    #[test]
    fn test_duration_in_days() {
        let result = eval_duration_in_days(&[json!("1984-10-11"), json!("1984-10-21")]).unwrap();
        let micros = result.as_i64().unwrap();
        assert_eq!(micros, 10 * MICROS_PER_DAY);
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
