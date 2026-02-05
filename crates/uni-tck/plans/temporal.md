# Plan: Fix Temporal Support in Cypher Query Engine

## Problem Summary

TCK temporal tests (999/1004 failing) fail because:
1. **Map-based constructors** like `date({year: 1984, month: 10})` aren't implemented
2. **Dotted functions** like `datetime.fromepoch()` don't exist

**Root Cause**: The parser works (100% pass), but `datetime.rs` functions only accept strings, not maps.

## Solution

Extend temporal functions in `datetime.rs` to handle `Value::Object(map)` inputs. The pattern already exists in `eval_duration()` which correctly handles maps.

## Files to Modify

| File | Changes |
|------|---------|
| `crates/uni-query/src/query/datetime.rs` | Add map handling + new functions |
| `crates/uni-query/src/query/expr_eval.rs` | Route dotted function names |

## Implementation Steps

### Step 1: Extend `eval_date()` for map input (~50 lines)

**File**: `crates/uni-query/src/query/datetime.rs`

Add `Value::Object(map)` handling to construct dates from:
- Basic: `year`, `month` (default 1), `day` (default 1)
- Week-based: `year`, `week`, `dayOfWeek`
- Ordinal: `year`, `ordinalDay`
- Quarter: `year`, `quarter`, `dayOfQuarter`

```rust
Value::Object(map) => {
    let year = map.get("year").and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("date() requires 'year'"))?;

    if let Some(week) = map.get("week").and_then(|v| v.as_i64()) {
        let dow = map.get("dayOfWeek").and_then(|v| v.as_i64()).unwrap_or(1);
        return construct_date_from_week(year, week, dow);
    }
    // ... similar for ordinalDay, quarter

    let month = map.get("month").and_then(|v| v.as_i64()).unwrap_or(1);
    let day = map.get("day").and_then(|v| v.as_i64()).unwrap_or(1);
    NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
}
```

### Step 2: Extend `eval_time()` for map input (~30 lines)

Add map handling with fields: `hour`, `minute`, `second`, `millisecond`, `microsecond`, `nanosecond`

### Step 3: Extend `eval_datetime()` for map input (~60 lines)

Combine date + time fields, add `timezone` support.

### Step 4: Extend `eval_localtime()` and `eval_localdatetime()` (~40 lines)

Similar to time/datetime but without timezone.

### Step 5: Add epoch functions (~30 lines)

```rust
fn eval_datetime_fromepoch(args: &[Value]) -> Result<Value> {
    let seconds = args[0].as_i64()?;
    let nanos = args[1].as_i64()?;
    let dt = DateTime::from_timestamp(seconds, nanos as u32)?;
    Ok(Value::String(dt.to_rfc3339()))
}

fn eval_datetime_fromepochmillis(args: &[Value]) -> Result<Value> {
    let millis = args[0].as_i64()?;
    let dt = DateTime::from_timestamp_millis(millis)?;
    Ok(Value::String(dt.to_rfc3339()))
}
```

### Step 6: Add truncation functions (~80 lines)

```rust
fn eval_datetime_truncate(args: &[Value]) -> Result<Value> {
    let unit = args[0].as_str()?;  // "day", "hour", "minute", etc.
    let dt = parse_datetime_utc(args[1].as_str()?)?;
    let truncated = truncate_to_unit(&dt, unit)?;
    Ok(Value::String(truncated.to_rfc3339()))
}
```

Units: `millennium`, `century`, `decade`, `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`, `millisecond`, `microsecond`

### Step 7: Update dispatcher in `eval_datetime_function()` (~10 lines)

```rust
match name {
    "DATE" => eval_date(args),
    // ... existing ...
    "DATETIME.FROMEPOCH" => eval_datetime_fromepoch(args),
    "DATETIME.FROMEPOCHMILLIS" => eval_datetime_fromepochmillis(args),
    "DATETIME.TRUNCATE" => eval_datetime_truncate(args),
    "DATE.TRUNCATE" => eval_date_truncate(args),
    "TIME.TRUNCATE" => eval_time_truncate(args),
    // ...
}
```

### Step 8: Update `expr_eval.rs` function routing (~5 lines)

**File**: `crates/uni-query/src/query/expr_eval.rs` (around line 1091)

Add dotted names to the datetime function matcher:
```rust
| "DATETIME.FROMEPOCH" | "DATETIME.FROMEPOCHMILLIS"
| "DATETIME.TRUNCATE" | "DATE.TRUNCATE" | "TIME.TRUNCATE"
```

## Helper Functions Needed

```rust
fn construct_date_from_week(year: i64, week: i64, day_of_week: i64) -> Result<Value>
fn construct_date_from_ordinal(year: i64, ordinal_day: i64) -> Result<Value>
fn construct_date_from_quarter(year: i64, quarter: i64, day_of_quarter: i64) -> Result<Value>
fn truncate_to_unit(dt: &DateTime<Utc>, unit: &str) -> Result<DateTime<Utc>>
```

## Verification

1. **Run TCK temporal tests**:
   ```bash
   cargo test -p uni-tck --test cucumber -- -i "features/expressions/temporal/*.feature"
   ```

2. **Run datetime unit tests**:
   ```bash
   cargo nextest run -p uni-query datetime
   ```

3. **Quick smoke test**:
   ```bash
   cargo test -p uni-tck --test cucumber -- --name "Should construct date"
   ```

## Expected Outcome

- ~800+ of 1004 temporal TCK tests should pass after implementing map constructors
- Remaining failures likely due to advanced features (named timezones like 'Europe/Stockholm')

## Risk Assessment

**Low Risk**: Changes isolated to 2 files, follows existing `eval_duration()` pattern.

## Implementation Order

1. Steps 1-4: Map-based constructors (highest TCK impact)
2. Steps 5-6: Epoch and truncation functions
3. Steps 7-8: Dispatcher updates
4. Run tests after each step
