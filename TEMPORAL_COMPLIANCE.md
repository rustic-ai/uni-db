# Temporal TCK Failure Analysis

## Overview

| Feature | Total | Pass | Fail | Rate |
|---------|-------|------|------|------|
| Temporal1 | 207 | 117 | **90** | 57% |
| Temporal2 | 53 | 16 | **37** | 30% |
| Temporal3 | 183 | 64 | **119** | 35% |
| Temporal4 | 39 | 30 | **9** | 77% |
| Temporal5 | 7 | 0 | **7** | 0% |
| Temporal6 | 17 | 17 | 0 | 100% |
| Temporal7 | 18 | 17 | **1** | 94% |
| Temporal8 | 27 | 0 | **27** | 0% |
| Temporal9 | 322 | 120 | **202** | 37% |
| Temporal10 | 131 | 35 | **96** | 27% |
| **Total** | **1004** | **416** | **588** | **41%** |

---

## Cluster 1: Compact/Non-Standard Date/Time Parsing (~55 failures)

**Affects**: Temporal2 (37), Temporal3 (partial), Temporal9 (17), Temporal10 (1)

**Error pattern**: `Invalid date format: premature end of input` or `Invalid time format`

**Root cause**: The `date()`, `localtime()`, and `*.truncate()` string parsers only handle ISO 8601 extended format (`YYYY-MM-DD`). They can't parse:
- Compact dates: `19840711` (YYYYMMDD without dashes)
- Ordinal dates: `1984183` (YYYY + day-of-year)
- ISO week dates: `1984W30` or `1984W302`
- Compact times without colons

**Solution**: Extend the temporal string parser to detect and handle all ISO 8601 basic formats.

---

## Cluster 2: ISO Week-Based Construction (90 failures)

**Affects**: Temporal1 (all 90 failures)

**Error pattern**: `Result mismatch` — week-based localdatetime/datetime produces wrong dates (e.g., actual `1816-01-01T00:00:00` vs expected different value)

**Root cause**: The `localdatetime({year, week, dayOfWeek})` and `datetime({year, week, dayOfWeek})` constructors don't correctly convert ISO week numbers to calendar dates. The ISO week-to-date algorithm is wrong or missing.

**Solution**: Fix ISO week date to calendar date conversion (ISO 8601 week date algorithm).

---

## Cluster 3: Temporal Select/Replace (Component Override) (~80+ failures)

**Affects**: Temporal3 (majority of 119 failures)

**Error pattern**: `Result mismatch` — when selecting/replacing temporal components, wrong values produced. E.g., `localdatetime({datetime: d, hour: 0})` returns `1984-10-11T00:00:00` instead of expected value with original minutes/seconds preserved, or vice versa.

**Root cause**: Temporal constructor "select" operations (override specific date/time components while preserving others from an input temporal) have incorrect logic. Component replacement doesn't preserve unmodified fields properly.

**Solution**: Fix the temporal constructor logic that handles `{date: <temporal>, day: 28}` or `{datetime: <temporal>, hour: 0}` style overrides to correctly carry forward unspecified components.

---

## Cluster 4: Temporal Property Accessors (7 failures)

**Affects**: Temporal5 (all 7 failures)

**Error pattern**: `TypeError: InvalidArgumentType - cannot index int`

**Root cause**: Property accessors on temporal values (`.year`, `.month`, `.day`, `.hour`, `.minute`, `.second`, `.week`, `.dayOfWeek`, `.dayOfYear`, `.offset`, etc.) are completely unimplemented. The engine treats temporal values as opaque strings, so `d.year` fails.

**Solution**: Implement dot-property access on temporal types that extracts the appropriate component. Requires recognizing temporal types and parsing them to extract components.

---

## Cluster 5: Temporal Arithmetic (27 failures)

**Affects**: Temporal8 (all 27 failures)

**Error pattern**: `Error during planning: Cannot coerce arithmetic expression Utf8 - Utf8`

**Root cause**: Temporal arithmetic (`date + duration`, `date - duration`, `time + duration`, etc.) is completely unimplemented. The planner sees both operands as `Utf8` and doesn't know how to subtract/add them.

**Solution**: Implement temporal arithmetic operators. The planner needs to recognize temporal + duration and temporal - duration patterns and emit appropriate computation logic.

---

## Cluster 6: Duration Between / Arrow Schema Mismatch (~70 failures)

**Affects**: Temporal10 (majority of 96 failures)

**Error pattern**: `Arrow error: Invalid argument error: column types must match schema`

**Root cause**: `duration.between()`, `duration.inMonths()`, `duration.inDays()`, `duration.inSeconds()` functions return a Duration value but the result column schema expects a different type (or vice versa). This is a type coercion issue in the executor's result construction.

**Solution**: Fix the return type of duration computation functions to produce a consistent Arrow schema. May need a dedicated Duration column type or ensure all branches return the same type.

---

## Cluster 7: Truncation Logic (Result Mismatches) (~185 failures)

**Affects**: Temporal9 (majority of 202 failures)

**Error pattern**: `Result mismatch` — truncation produces wrong values, e.g.:
- `datetime.truncate('day', ...)` returns wrong date (timezone offset not applied before truncation)
- `localtime.truncate('second', ...)` returns `12:31:14.000000002` instead of `12:31:14`
- `time.truncate('hour', ...)` returns `12:00Z` instead of `12:00+01:00` (timezone not preserved)

**Root cause**: Multiple sub-issues:
1. Timezone offsets not applied before truncation (truncate in UTC instead of local)
2. Nanosecond precision not properly zeroed in truncation
3. Truncation to 'week' not implemented or wrong
4. Timezone not preserved in output after truncation

**Solution**: Fix truncation to (a) apply timezone offset before truncating, (b) properly zero sub-precision components, (c) preserve original timezone in output.

---

## Cluster 8: Temporal Output Formatting (~10 failures)

**Affects**: Temporal4 (9), Temporal7 (1)

**Error pattern**: `Result mismatch` — stored/compared temporal values have wrong format. E.g., `time("12:00")` should output as `12:00:00+00:00` (with offset for Time type) but outputs `12:00`.

**Root cause**: Temporal value serialization doesn't follow Cypher conventions:
- `Time` type should always include timezone offset
- Missing precision normalization (trailing zeros)
- LocalDateTime vs DateTime formatting differences

**Solution**: Fix temporal value formatting to match Cypher output conventions for each temporal type.

---

## Priority / Impact Matrix

| Cluster | Failures Fixed | Difficulty | Priority |
|---------|---------------|------------|----------|
| 7: Truncation Logic | ~185 | Medium | **Highest** (most failures) |
| 2: Week-Based Construction | ~90 | Low-Medium | **High** |
| 3: Select/Replace | ~80+ | Medium | **High** |
| 6: Duration Between Schema | ~70 | Medium | **High** |
| 1: Compact Date Parsing | ~55 | Low | **Medium** |
| 5: Temporal Arithmetic | 27 | Medium-High | **Medium** |
| 8: Output Formatting | ~10 | Low | **Low** |
| 4: Property Accessors | 7 | Medium | **Low** |

Fixing clusters 7, 2, 3, and 6 would resolve ~425 of the 588 failures (~72%). Adding cluster 1 brings it to ~480 (~82%).
