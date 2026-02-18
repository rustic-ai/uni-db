# Temporal Type Fix Progress Report

**Date:** 2026-02-17
**Baseline:** 3052/3897 TCK scenarios passing (78.3%)
**Current:** 3395/3897 TCK scenarios passing (87.1%)
**Improvement:** +343 scenarios fixed

## Summary of Changes Made

### Phase 1: Fix Temporal Struct Identity Loss in `arrow_to_value` (DONE)

**File:** `crates/uni-store/src/storage/arrow_convert.rs:312`

- Added temporal struct pattern detection **before** the generic StructArray→Map handler
- Detects DateTime structs by field names: `{nanos_since_epoch, offset_seconds, timezone_name}`
- Detects Time structs by field names: `{nanos_since_midnight, offset_seconds}`
- Falls through to generic Map handler for non-temporal structs
- Handles both standard Arrow types (TimestampNanosecond, Time64Nanosecond) and Int64 fallback

### Phase 2: Fix Temporal Accessor Routing (DONE)

**Files:** `datetime.rs`, `df_udfs.rs`, `df_expr.rs`

- **2.1:** `is_temporal_accessor()` already existed (lines 577-604)
- **2.2:** Added `eval_temporal_accessor_value(val: &Value, component: &str)` — handles `Value::Temporal`, `Value::String`, `Value::Null`. Extracts offset/timezone/epoch directly from TemporalValue fields to avoid lossy string round-trip
- **2.3:** Created `_temporal_property` UDF in `df_udfs.rs` — VariadicAny signature, returns LargeBinary, delegates to `eval_temporal_accessor_value()`
- **2.4:** Routed temporal accessors in `translate_property_access()` — both variable and complex-expression branches now check `is_temporal_accessor()` before falling through to `index()` UDF
- **2.5:** Fixed `_duration_property` UDF to accept `Value::Temporal(Duration{..})` by converting to string via `.to_string()`
- **Added `quarters` accessor** to `is_duration_accessor()` and `eval_duration_accessor()` (was missing, needed by Temporal5 tests)

### Phase 3: Fix Temporal Projection Cross-Type Sources (DONE)

**File:** `datetime.rs`

- Fixed `eval_time_from_projection()` to handle DateTime, LocalDateTime, and Date sources (previously only Time/LocalTime/String)
- Fixed `eval_datetime_from_map()` to properly combine separate `date` and `time` keys via new `eval_datetime_from_date_and_time()` function
- Added `extract_time_and_tz_from_value()` helper for extracting time+timezone from temporal values
- Fixed `eval_datetime_from_map()` `time` key handling: when map has `{year:..., time: otherTime}`, extracts time component and applies overrides
- Fixed sub-second precision leaking: when explicit time fields (hour/minute/second) override the source, nanoseconds now default to 0 instead of inheriting from source
- Added cross-type conversion to all temporal constructors:
  - `date(datetime_val)` → extracts date component
  - `time(localtime_val)` → extracts time, adds UTC offset
  - `localtime(datetime_val)` → extracts time, strips timezone
  - `localdatetime(datetime_val)` → extracts date+time, strips timezone
  - `datetime(localdatetime_val)` → adds UTC timezone

### Phase 4: Fix `toString()` for Temporal Types (DONE)

**Files:** `df_udfs.rs`, `df_expr.rs`

- Created `tostring` UDF that handles Temporal, String, Int, Float, Bool, Null
- Errors on invalid types (List, Map, Node, Edge, Path) per Cypher spec
- Changed `toString()` from `cast_expr(..., Utf8)` to `dummy_udf_expr("tostring", df_args)`

### Additional Fixes

- **Temporal9 truncate_time:** Fixed timezone extraction to handle `TemporalValue::DateTime` variant (was only handling `TemporalValue::Time`, causing timezone loss)
- **Source timezone propagation:** `eval_datetime_from_map()` now inherits timezone from `time` key source when no explicit `timezone` is provided

## Current Failure Counts

| Feature | Before | After | Delta |
|---------|--------|-------|-------|
| Temporal2 | 27 | 27 | 0 |
| Temporal3 | 146 | 30 | **-116** |
| Temporal5 | 7 | 1 | **-6** |
| Temporal6 | 14 | 0 | **-14** |
| Temporal7 | 2 | 2 | 0 |
| Temporal8 | 6 | 0 | **-6** |
| Temporal9 | 130 | 0 | **-130** |
| Temporal10 | 80 | 7 | **-73** |
| **Total** | **412** | **67** | **-345** |

## Remaining Failures & Next Steps

### Temporal3 (30 remaining)
Root causes identified:
1. **Timezone switching** (~20): When constructing a temporal with a different timezone than the source, the implementation replaces the offset label without adjusting the local time to preserve the UTC instant. Fix: in `eval_datetime_from_date_and_time()` and `eval_datetime_from_projection()`, when `timezone` differs from source, convert source to UTC first, then re-localize in the new timezone.
2. **Quarter override in date projection** (~3): `date({date: other, quarter: 3})` goes to start of quarter instead of preserving month-within-quarter position and day.
3. **Misc edge cases** (~7): Various remaining issues in cross-type projections.

### Temporal5 (1 remaining)
- The duration accessor test `[7]` for `d.quarters` — likely a Cypher string representation issue where the duration value stored as a property comes back as a string that doesn't parse correctly, or there's a test comparison mismatch.

### Temporal2 (27 remaining — not yet investigated)
- These are string parsing / duration edge cases. Likely:
  - Compact ISO 8601 date parsing (e.g., `20191101T12:00`)
  - Duration formatting edge cases
  - Negative duration handling

### Temporal7 (2 remaining)
- Duration equality comparison edge cases

### Temporal10 (7 remaining)
Root causes identified:
1. **Extreme year range** (2): `date('-999999999-01-01')` parsing not supported by chrono
2. **`duration.inSeconds()` return type mismatch** (5): When both arguments are no-arg constructor calls (`duration.inSeconds(localtime(), localtime())`), the return type is `Interval(MonthDayNano)` instead of `Int64`. Likely a DataFusion optimization that short-circuits identical expressions.

## Phase 5-7 Plan

### Phase 5: Fix Duration Display Formatting (Temporal2)
- Investigate the 27 Temporal2 failures for specific format/parse issues
- Likely compact ISO 8601 parsing and negative duration edge cases

### Phase 6: Fix Remaining Issues
- Timezone switching logic in datetime projection (Temporal3)
- Quarter override logic in date projection (Temporal3)
- `duration.inSeconds()` return type for identical operands (Temporal10)

### Phase 7: Full Validation
- Run full TCK
- Run all unit tests
- Compare final pass rates

## Files Modified

| File | Changes |
|------|---------|
| `crates/uni-store/src/storage/arrow_convert.rs` | Temporal struct detection in `arrow_to_value()` |
| `crates/uni-query/src/query/df_expr.rs` | Temporal accessor routing + `toString()` UDF |
| `crates/uni-query/src/query/df_udfs.rs` | `_temporal_property`, `tostring` UDFs, `_duration_property` fix |
| `crates/uni-query/src/query/datetime.rs` | `eval_temporal_accessor_value()`, cross-type constructors, `eval_datetime_from_date_and_time()`, `extract_time_and_tz_from_value()`, truncate_time offset fix, sub-second precision fix, `quarters` accessor |

## Test Status
- All 1464 unit tests pass (0 failures)
- TCK: 3395/3897 passing (87.1%), up from 3052 (78.3%)
