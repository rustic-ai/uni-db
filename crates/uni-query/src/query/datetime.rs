// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use serde_json::{Value, json};

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

/// Evaluate date/time functions: DATE, TIME, DATETIME, DURATION, and extraction functions.
pub fn eval_datetime_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "DATE" => eval_date(args),
        "TIME" => eval_time(args),
        "DATETIME" => eval_datetime(args),
        "LOCALDATETIME" => eval_localdatetime(args),
        "LOCALTIME" => eval_localtime(args),
        "DURATION" => eval_duration(args),
        "YEAR" => eval_extract(args, Component::Year),
        "MONTH" => eval_extract(args, Component::Month),
        "DAY" => eval_extract(args, Component::Day),
        "HOUR" => eval_extract(args, Component::Hour),
        "MINUTE" => eval_extract(args, Component::Minute),
        "SECOND" => eval_extract(args, Component::Second),
        _ => Err(anyhow!("Unknown datetime function: {}", name)),
    }
}

enum Component {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

fn eval_date(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        // Current date
        let now = Utc::now().date_naive();
        return Ok(Value::String(now.format("%Y-%m-%d").to_string()));
    }
    match &args[0] {
        Value::String(s) => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| dt.date())
                }) // Handle timestamp string
                .map_err(|e| anyhow!("Invalid date format: {}", e))?;
            Ok(Value::String(date.format("%Y-%m-%d").to_string()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("date() expects a string argument")),
    }
}

fn eval_time(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        // Current time
        let now = Utc::now().time();
        return Ok(Value::String(now.format("%H:%M:%S%.6f").to_string()));
    }
    match &args[0] {
        Value::String(s) => {
            // Try parsing just time, or extract time from datetime string
            let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| dt.time())
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").map(|dt| dt.time())
                })
                .or_else(|_| DateTime::parse_from_rfc3339(s).map(|dt| dt.time()))
                .map_err(|_| anyhow!("Invalid time format"))?;
            Ok(Value::String(time.format("%H:%M:%S%.6f").to_string()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("time() expects a string argument")),
    }
}

fn eval_datetime(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        // Current datetime
        let now = Utc::now();
        return Ok(Value::String(now.to_rfc3339()));
    }
    match &args[0] {
        Value::String(s) => {
            // Parse and return ISO 8601 string
            let dt: DateTime<Utc> = parse_datetime_utc(s)?;
            Ok(Value::String(dt.to_rfc3339()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("datetime() expects a string argument")),
    }
}

fn eval_localdatetime(args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(anyhow!("localdatetime() takes no arguments"));
    }
    let now = chrono::Local::now();
    Ok(Value::String(now.to_rfc3339()))
}

fn eval_localtime(args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(anyhow!("localtime() takes no arguments"));
    }
    let now = chrono::Local::now().time();
    Ok(Value::String(now.format("%H:%M:%S%.6f").to_string()))
}

fn eval_duration(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("duration() requires 1 argument"));
    }
    match &args[0] {
        Value::String(s) => {
            // Parse ISO 8601 duration format (P1DT1H30M etc) or simple formats
            let micros = parse_duration_to_micros(s)?;
            Ok(json!(micros))
        }
        Value::Object(map) => {
            // duration({days: 1, hours: 2, minutes: 30, seconds: 15})
            // Convert to microseconds
            let mut total_micros: i64 = 0;

            if let Some(days) = map.get("days").and_then(|v| v.as_i64()) {
                total_micros += days * 24 * 60 * 60 * 1_000_000;
            }
            if let Some(hours) = map.get("hours").and_then(|v| v.as_i64()) {
                total_micros += hours * 60 * 60 * 1_000_000;
            }
            if let Some(minutes) = map.get("minutes").and_then(|v| v.as_i64()) {
                total_micros += minutes * 60 * 1_000_000;
            }
            if let Some(seconds) = map.get("seconds").and_then(|v| v.as_i64()) {
                total_micros += seconds * 1_000_000;
            }
            if let Some(millis) = map.get("milliseconds").and_then(|v| v.as_i64()) {
                total_micros += millis * 1_000;
            }
            if let Some(micros) = map.get("microseconds").and_then(|v| v.as_i64()) {
                total_micros += micros;
            }

            Ok(json!(total_micros))
        }
        Value::Number(n) => {
            // If already a number, assume it's microseconds
            Ok(Value::Number(n.clone()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("duration() expects a string, map, or number")),
    }
}

/// Parse a duration string to microseconds.
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

/// Parse ISO 8601 duration format (e.g., "P1DT2H30M15S")
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

        if c.is_ascii_digit() || c == '.' {
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
                'Y' | 'y' => (num * 365.0 * 24.0 * 60.0 * 60.0 * 1_000_000.0) as i64, // Approximate
                'D' | 'd' if !in_time_part => (num * 24.0 * 60.0 * 60.0 * 1_000_000.0) as i64,
                'H' | 'h' => (num * 60.0 * 60.0 * 1_000_000.0) as i64,
                'M' | 'm' if in_time_part => (num * 60.0 * 1_000_000.0) as i64,
                'S' | 's' => (num * 1_000_000.0) as i64,
                _ => return Err(anyhow!("Invalid ISO 8601 duration format")),
            };
            total_micros += micros;
        }
    }

    Ok(total_micros)
}

/// Parse simple duration format (e.g., "1d2h30m15s", "90s", "1h30m")
fn parse_simple_duration(s: &str) -> Result<i64> {
    let mut total_micros: i64 = 0;
    let mut num_buf = String::new();

    for c in s.chars() {
        if c.is_ascii_digit() || c == '.' {
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
                'd' => (num * 24.0 * 60.0 * 60.0 * 1_000_000.0) as i64,
                'h' => (num * 60.0 * 60.0 * 1_000_000.0) as i64,
                'm' => (num * 60.0 * 1_000_000.0) as i64,
                's' => (num * 1_000_000.0) as i64,
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
        total_micros += (num * 1_000_000.0) as i64;
    }

    Ok(total_micros)
}

fn eval_extract(args: &[Value], component: Component) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("Extract function requires 1 argument"));
    }
    match &args[0] {
        Value::String(s) => {
            // Try parsing as DateTime, then NaiveDateTime, then NaiveDate (for Year/Month/Day), then NaiveTime (for Hour/Min/Sec)
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

/// Check if value is a datetime string
pub fn is_datetime_value(val: &Value) -> bool {
    match val {
        Value::String(s) => parse_datetime_utc(s).is_ok(),
        _ => false,
    }
}

/// Check if value is a date string
pub fn is_date_value(val: &Value) -> bool {
    match val {
        Value::String(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok(),
        _ => false,
    }
}

/// Check if value is a duration (i64 microseconds)
pub fn is_duration_value(val: &Value) -> bool {
    // We store duration as a Number (i64)
    val.is_i64()
}

/// Add duration (microseconds) to datetime
pub fn add_duration_to_datetime(dt_str: &str, micros: i64) -> Result<String> {
    let dt = parse_datetime_utc(dt_str)?;
    let result = dt + chrono::Duration::microseconds(micros);
    Ok(result.to_rfc3339())
}

/// Add duration (microseconds) to date
pub fn add_duration_to_date(date_str: &str, micros: i64) -> Result<String> {
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?;
    let dt = date.and_hms_opt(0, 0, 0).unwrap();
    let result = dt + chrono::Duration::microseconds(micros);
    Ok(result.format("%Y-%m-%d").to_string())
}

/// Subtract two datetimes, return duration in microseconds
pub fn datetime_difference(dt1_str: &str, dt2_str: &str) -> Result<i64> {
    let dt1 = parse_datetime_utc(dt1_str)?;
    let dt2 = parse_datetime_utc(dt2_str)?;
    dt1.signed_duration_since(dt2)
        .num_microseconds()
        .ok_or_else(|| anyhow!("Duration overflow"))
}
