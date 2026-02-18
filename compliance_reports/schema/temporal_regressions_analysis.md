# Temporal Regression Analysis

**Source:** `compliance_reports/schema/report.md` (2026-02-15 17:01:53)
**Total temporal regressions:** ~462 scenarios across Temporal1-10 + WithOrderBy1

## Summary by Feature

| Feature | Regressed | Total Failed | Pass Rate | Delta |
|---------|-----------|--------------|-----------|-------|
| Temporal1 | ~120 | 153/207 | 26% | -70pp |
| Temporal2 | ~9 | 40/53 | 25% | -6pp |
| Temporal3 | ~30+ | 139/183 | 24% | -15pp |
| Temporal4 | ~12 | 12/39 | 69% | -23pp |
| Temporal5 | 0 (pre-existing) | 7/7 | 0% | -- |
| Temporal6 | ~14 | 14/17 | 18% | -82pp |
| Temporal7 | 0 (improved) | 0/18 | 100% | +6pp |
| Temporal8 | 0 (improved) | 6/27 | 78% | +78pp |
| Temporal9 | ~200+ | 242/322 | 25% | -75pp |
| Temporal10 | ~15 | 42/131 | 68% | +41pp |
| WithOrderBy1 | 7 | -- | -- | -- |

---

## Root Cause Groups

### Group 1: TCK Comparison Boundary -- Temporal values returned as opaque structs instead of formatted strings (~350 scenarios)

**Error pattern:**
```
Result mismatch (any order): No match found for actual row 0.
Actual values: [("result", Some(Temporal(LocalDateTime { nanos_sinc...
```

The engine returns the correct temporal value internally (as `Temporal(DateTime { nanos_since_epoch: ... })`, `Temporal(LocalTime { nanos_since_midnight: ... })`, etc.) but the TCK comparison step cannot match it against the expected Cypher string representation (e.g. `'2015-07-21T21:40:32.142+01:00'`).

**Root cause:** The `nanos_since_epoch` migration changed the internal representation, and the TCK harness comparison logic in `crates/uni-tck/src/steps/then.rs` now sees the raw struct debug format instead of converting temporals to their ISO 8601 string form before comparison. The `Value::Temporal(...)` variant isn't being formatted to a displayable string at the comparison boundary.

**Affected scenarios (all types, all operations):**
- **Temporal1** [2] construct week localdatetime (15 lines)
- **Temporal1** [3] construct week datetime (15 lines)
- **Temporal1** [5] construct local time (2 lines)
- **Temporal1** [6] construct time (14 lines)
- **Temporal1** [7] construct local date time (13 lines)
- **Temporal1** [8] construct date time with default time zone (30 lines)
- **Temporal1** [9] construct date time with offset time zone (29 lines)
- **Temporal1** [10] construct date time with named time zone (29 lines)
- **Temporal1** [13] construct temporal with time offset with second precision (4 lines)
- **Temporal2** [1] parse date from string (2 lines -- comparison mismatch subset)
- **Temporal2** [2] parse local time from string (1 regressed line)
- **Temporal2** [3] parse time from string (2 regressed lines)
- **Temporal2** [5] parse date time from string (1 regressed line)
- **Temporal2** [6] parse date time with named time zone from string (5 regressed lines)
- **Temporal3** [1] select date (subset with comparison mismatch)
- **Temporal3** [2] select local time (3 lines -- comparison mismatch subset)
- **Temporal3** [3] select time (subset with comparison mismatch)
- **Temporal3** [4] select date into local date time (all lines)
- **Temporal3** [5] select time into local date time (all lines)
- **Temporal3** [6] select date and time into local date time (2 regressed lines)
- **Temporal3** [7] select datetime into local date time (3 regressed lines)
- **Temporal3** [11] datetime into date time (4 regressed lines)
- **Temporal4** [3] store local time (value round-trips as String("12:00:00"))
- **Temporal4** [4] store local time array
- **Temporal4** [5-6] store time / time array
- **Temporal4** [7-8] store local date time / array
- **Temporal4** [9-10] store date time / array
- **Temporal6** [3] serialize time (1 line)
- **Temporal6** [5] serialize date time (1 line)
- **Temporal6** [7] serialize timezones correctly (1 line)
- **Temporal8** [3] add/subtract duration to/from time (3 lines)
- **Temporal8** [5] add/subtract duration to/from date time (3 lines)
- **Temporal9** [2] truncate datetime (~100 lines)
- **Temporal9** [3] truncate localdatetime (~60+ lines)
- **Temporal9** [4] truncate localtime (~30 lines)
- **Temporal9** [5] truncate time (~48 lines)
- **Temporal10** [2] compute duration between two temporals (9 lines)
- **Temporal10** [5] compute duration between two temporals in seconds (some lines)
- **WithOrderBy1** [39-42] sort by local date time / date time property (7 lines)

**Fix:** Fix the `Display`/formatting impl or the TCK comparison function so that `Value::Temporal(...)` variants are converted to their canonical ISO 8601 string before comparing with expected values. This is a single fix at the conversion/presentation boundary.

---

### Group 2: Duration parsing -- "Invalid duration format" (~11 scenarios)

**Error pattern:**
```
Query returned error instead of result: Query { message: "Execution error: duration(): Invalid duration format" ...
```

**Affected scenarios:**
- **Temporal6** [6] serialize duration (11 lines: 107-117)

**Root cause:** The `duration()` function's string parser rejects duration formats that were previously accepted. Likely the parser doesn't handle all ISO 8601 duration variants (e.g. fractional seconds, date-based durations like `P14DT16H12M`).

**Fix:** Expand the duration string parser to accept the full range of ISO 8601 duration formats.

---

### Group 3: Duration property access -- "duration must be a string" (~6 scenarios)

**Error pattern:**
```
Query returned error instead of result: Query { message: "Execution error: _duration_property(): duration must be a string" ...
```

**Affected scenarios:**
- **Temporal10** [1] split between boundaries correctly (6 lines: 47-52)

**Root cause:** When accessing properties of a duration (e.g. `d.months`, `d.days`), the `_duration_property()` function expects the duration to be a string but receives the internal `Duration` struct. The property accessor doesn't handle the native `Temporal(Duration { ... })` value type.

**Fix:** Make `_duration_property()` accept `Value::Temporal(Duration { ... })` directly instead of requiring a string.

---

### Group 4: Temporal accessor properties -- "cannot index into temporal" (~7 scenarios, pre-existing)

**Error pattern:**
```
Query returned error instead of result: Type { expected: "Execution error: TypeError: InvalidArgumentType - cannot index int...
```

**Affected scenarios:**
- **Temporal5** [1-7] accessors for date, local time, time, local date time, date time, duration (7 lines)

**Root cause:** Property access on temporal values (e.g. `d.year`, `t.hour`, `dt.timezone`) is not implemented -- temporal values can't be "indexed" by property name. This appears to be a **pre-existing gap** (Temporal5 was already at 0%), not a new regression.

**Fix:** Implement temporal property accessors (`year`, `month`, `day`, `hour`, `minute`, `second`, `millisecond`, `microsecond`, `nanosecond`, `timezone`, `offset`, `epochMillis`, `epochSeconds`, etc.) on the temporal value types.

---

### Group 5: Temporal constructor type coercion -- "expects a string or map argument" (~30+ scenarios)

**Error pattern:**
```
Query returned error instead of result: Query { message: "Execution error: date(): date() expects a string or map argument" ...
Query returned error instead of result: Query { message: "Execution error: localtime(): localtime() expects a string or map argument" ...
Query returned error instead of result: Query { message: "Execution error: time(): time() expects a string or map argument" ...
```

**Affected scenarios:**
- **Temporal3** [1] select date (2 lines where `date()` receives a temporal not string/map)
- **Temporal3** [2] select local time (3 lines where `localtime()` receives a temporal)
- **Temporal3** [3] select time (many lines where `time()` receives a temporal)

**Root cause:** When a temporal constructor like `date()`, `localtime()`, `time()`, `datetime()` receives another temporal value as argument (e.g. `date(localdatetime(...))` to extract the date component), the constructor rejects it because it only checks for string or map arguments. In Cypher, temporal constructors should accept other temporals and extract the relevant fields.

**Fix:** Extend temporal constructor functions to accept `Value::Temporal(...)` inputs and extract the relevant components (date part, time part, etc.).

---

### Group 6: Temporal field extraction -- "time field must be a string or temporal" (~15+ scenarios)

**Error pattern:**
```
Query returned error instead of result: Query { message: "Execution error: localtime(): time field must be a string or temporal" ...
Query returned error instead of result: Query { message: "Execution error: time(): time field must be a string or temporal" ...
```

**Affected scenarios:**
- **Temporal3** [2] select local time (lines 89-93)
- **Temporal3** [3] select time (lines 120-128)

**Root cause:** When constructing a temporal from a map with `time: <value>` field, the function expects the `time` field to be a string or temporal, but receives something else (likely the internal representation changed). Related to Group 5 -- the internal temporal values aren't being recognized as "temporal" by the type-check.

**Fix:** Same as Group 5 -- ensure the type-check for temporal fields recognizes `Value::Temporal(...)`.

---

### Group 7: Week-based date parsing -- "Cannot parse datetime" (~4 scenarios)

**Error pattern:**
```
Query returned error instead of result: Query { message: "Execution error: date(): Invalid date format: Cannot parse datetime...
Query returned error instead of result: Query { message: "Execution error: localdatetime(): Cannot parse datetime: 2015-W30...
```

**Affected scenarios:**
- **Temporal2** [1] parse date from string (3 lines: week-date format `2015-W30-2`)
- **Temporal2** [4] parse local date time from string (week-date based)

**Root cause:** The date/time parser doesn't support ISO 8601 week-date format (`YYYY-Www-D`). It tries to parse `2015-W30-2` as a regular date and fails.

**Fix:** Add week-date format parsing (`YYYY-Www-D`, `YYYY-Www`) to the temporal string parser.

---

### Group 8: Compact time format parsing -- "Cannot parse datetime: 2140-..." (~4 scenarios)

**Error pattern:**
```
Query returned error instead of result: Query { message: "Execution error: time(): Cannot parse datetime: 21:40-01:30" ...
Query returned error instead of result: Query { message: "Execution error: time(): Cannot parse datetime: 2140-00:00" ...
Query returned error instead of result: Query { message: "Execution error: time(): Cannot parse datetime: 2140-02" ...
Query returned error instead of result: Query { message: "Execution error: time(): Cannot parse datetime: 22+18:00" ...
```

**Affected scenarios:**
- **Temporal2** [3] parse time from string (lines 96-99)

**Root cause:** The time parser doesn't handle compact time formats (without colons, e.g. `2140` for `21:40`) or time-with-offset formats where the offset uses shorthand (e.g. `22+18:00`, `2140-02`).

**Fix:** Extend the time parser to handle compact/no-colon formats and timezone offset shorthand.

---

### Group 9: Epoch construction -- datetime from epoch (1 scenario)

**Error pattern:** Comparison mismatch on `datetime({epochMillis: ...})` / `datetime({epochSeconds: ...})`

**Affected scenarios:**
- **Temporal1** [11] construct date time from epoch (line 366)

**Fix:** Likely falls under Group 1 (comparison boundary) but may also have construction issues with epoch-based temporal creation.

---

### Group 10: Duration construction from component map (1 scenario)

**Error pattern:** Comparison mismatch on `duration({...})` with various component fields.

**Affected scenarios:**
- **Temporal1** [12] construct duration (line 393)

**Fix:** Likely falls under Group 1 (comparison boundary).

---

## Priority Fix Order

| Priority | Group | Scenarios Fixed | Effort | Description |
|----------|-------|-----------------|--------|-------------|
| **P0** | 1 | ~350 | Low | Fix TCK comparison/Display for temporal values |
| **P1** | 5+6 | ~45 | Medium | Accept temporal values in constructor functions |
| **P2** | 3 | ~6 | Low | Fix `_duration_property()` to accept native Duration |
| **P3** | 2 | ~11 | Medium | Expand duration format parser |
| **P4** | 7 | ~4 | Medium | Add week-date format parsing |
| **P5** | 8 | ~4 | Medium | Add compact time format parsing |
| **P6** | 4 | ~7 | High | Implement temporal property accessors (pre-existing) |

**P0 alone would fix ~75% of all temporal regressions.** Combined P0+P1 covers ~85%. The remaining groups are parsing edge cases and property accessors.
