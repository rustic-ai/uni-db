# Temporal Intervals (BTIC)

Uni treats temporal intervals as a first-class data type. The **Binary Temporal Interval Codec (BTIC)** encodes half-open time intervals `[lo, hi)` with per-bound granularity and epistemic certainty into a single 24-byte property value. This guide covers creating intervals, querying with predicates, set operations, aggregation, and schema design.

## Why BTIC?

Real-world temporal data is messy. Historical dates have variable precision ("sometime in the 1400s"), scientific observations have uncertainty margins, and ongoing events have no known end. Traditional `start_date`/`end_date` column pairs lose this metadata.

BTIC solves this by encoding three things in one value:

| Component | What it captures | Example |
|-----------|-----------------|---------|
| **Bounds** | Half-open interval `[lo, hi)` in milliseconds since epoch | `[1985-01-01, 1986-01-01)` |
| **Granularity** | Precision of each bound (year, month, day, ..., millisecond) | "this was recorded as a year, not 365 days" |
| **Certainty** | Epistemic confidence in each bound (definite, approximate, uncertain, unknown) | "the start is approximate" |

---

## Setting Up

### Schema Definition

=== "Python"
    ```python
    db.schema() \
        .label("Event") \
            .property("name", DataType.STRING()) \
            .property("period", DataType.BTIC()) \
        .apply()
    ```

=== "Rust"
    ```rust
    db.schema()
        .label("Event")
            .property("name", DataType::String)
            .property("period", DataType::Btic)
        .apply().await?;
    ```

For optional temporal properties, use `.property_nullable("period", DataType.BTIC())`.

---

## Creating Intervals

The `btic()` constructor accepts ISO 8601-inspired string literals:

```cypher
// Year precision: [1985-01-01, 1986-01-01)
RETURN btic('1985')

// Month precision: [1985-03-01, 1985-04-01)
RETURN btic('1985-03')

// Day precision: [1985-03-15, 1985-03-16)
RETURN btic('1985-03-15')

// Timestamp to the second: [14:30:00, 14:30:01)
RETURN btic('1985-03-15T14:30:00Z')

// With milliseconds: [that millisecond, +1ms)
RETURN btic('1985-03-15T14:30:00.123Z')
```

### Ranges (solidus notation)

Use `/` to specify both bounds independently, with mixed granularity:

```cypher
// Year-to-year: [1939-01-01, 1946-01-01)
RETURN btic('1939/1945')

// Month-to-day: [1985-03-01, 2024-06-16)
RETURN btic('1985-03/2024-06-15')
```

### Certainty Annotations

Prefix a bound with `~` (approximate), `?` (uncertain), or `??` (unknown):

```cypher
// Approximate year: "around 1985"
RETURN btic('~1985')

// Uncertain start, definite end
RETURN btic('?1400/1453')
```

### Unbounded Intervals

```cypher
// Ongoing (no known end): [2020-03-01, +infinity)
RETURN btic('2020-03/')

// Unknown start: (-infinity, 2024-07-01)
RETURN btic('/2024-06')

// Fully unbounded
RETURN btic('/')
```

### BCE Dates

```cypher
// 500 BCE (astronomical year -499)
RETURN btic('500 BCE')

// Approximate BCE date
RETURN btic('~500 BCE')
```

### Storing Intervals

```cypher
// In CREATE
CREATE (e:Event {name: 'WW2', period: btic('1939/1945')})

// Via SET
MATCH (e:Event {name: 'WW2'}) SET e.period = btic('1939-09/1945-09')

// Via parameter
MATCH (e:Event {name: 'WW2'}) SET e.period = $interval
```

=== "Python"
    ```python
    from uni_db import Btic

    tx.execute(
        "CREATE (e:Event {name: 'Renaissance', period: $p})",
        params={"p": Btic("1400/1600")}
    )
    ```

---

## Accessor Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `btic_lo(b)` | DateTime | Lower bound (inclusive); NULL if unbounded |
| `btic_hi(b)` | DateTime | Upper bound (exclusive); NULL if unbounded. A bound outside the datetime range (about years 1677 to 2262), such as `btic('500 BCE')`, is an error; read it with `btic_duration` or the BTIC predicates instead |
| `btic_duration(b)` | Int64 | Duration in milliseconds; NULL if unbounded |
| `btic_granularity(b)` | String | Lower bound granularity (`"year"`, `"month"`, `"day"`, ...) |
| `btic_lo_granularity(b)` | String | Lower bound granularity |
| `btic_hi_granularity(b)` | String | Upper bound granularity |
| `btic_certainty(b)` | String | Least-certain of both bounds |
| `btic_lo_certainty(b)` | String | Lower bound certainty |
| `btic_hi_certainty(b)` | String | Upper bound certainty |
| `btic_is_finite(b)` | Boolean | True if both bounds are finite |
| `btic_is_unbounded(b)` | Boolean | True if either bound is infinite |
| `btic_is_instant(b)` | Boolean | True if interval is 1ms wide |

```cypher
MATCH (e:Event)
RETURN e.name,
       btic_lo(e.period) AS start,
       btic_hi(e.period) AS end,
       btic_granularity(e.period) AS precision,
       btic_certainty(e.period) AS confidence
```

---

## Predicates

### Point-in-Interval

```cypher
// Is a timestamp inside the interval?
MATCH (e:Event)
WHERE btic_contains_point(e.period, datetime('1942-06-15T00:00:00Z'))
RETURN e.name
```

`btic_contains_point` accepts Int64 (milliseconds since epoch), DateTime, LocalDateTime, and Date as the point argument.

### Interval Relationships (Allen's Algebra)

BTIC implements the full set of [Allen's interval algebra](https://en.wikipedia.org/wiki/Allen%27s_interval_algebra) predicates:

| Function | True when | Diagram |
|----------|-----------|---------|
| `btic_overlaps(a, b)` | Intervals share at least one tick | `a: [----)` / `b:   [----)` |
| `btic_contains(a, b)` | `a` fully contains `b` | `a: [---------)` / `b:   [---)` |
| `btic_before(a, b)` | `a` ends at or before `b` starts | `a: [----)   b: [----)` |
| `btic_after(a, b)` | `a` starts at or after `b` ends | `b: [----)   a: [----)` |
| `btic_meets(a, b)` | `a.hi == b.lo` (adjacent, no gap) | `a: [----)b: [----)` |
| `btic_adjacent(a, b)` | Either meets or met-by (symmetric) | |
| `btic_disjoint(a, b)` | No shared ticks | `a: [----)    b: [----)` |
| `btic_equals(a, b)` | Same bounds (ignoring metadata) | `a: [----)` / `b: [----)` |
| `btic_starts(a, b)` | Same `lo`, `a` ends earlier | `a: [---)` / `b: [-------)` |
| `btic_during(a, b)` | `a` strictly inside `b` | `a:   [---)` / `b: [-------)` |
| `btic_finishes(a, b)` | Same `hi`, `a` starts later | `a:     [---)` / `b: [-------)` |

```cypher
// Find events that overlap with World War 2
MATCH (e:Event)
WHERE btic_overlaps(e.period, btic('1939/1945'))
RETURN e.name

// Find events strictly during the Renaissance
MATCH (e:Event)
WHERE btic_during(e.period, btic('1400/1600'))
RETURN e.name
```

### Comparison Operators

Standard Cypher comparison operators work on BTIC values using the canonical total order (primary by lower bound, secondary by upper bound):

```cypher
// Events before the year 2000
MATCH (e:Event) WHERE e.period < btic('2000') RETURN e.name

// Events in or after 1985
MATCH (e:Event) WHERE e.period >= btic('1985') RETURN e.name

// Inequality
MATCH (e:Event) WHERE e.period <> btic('1990') RETURN e.name
```

---

## Set Operations

| Function | Returns | Description |
|----------|---------|-------------|
| `btic_intersection(a, b)` | Btic or NULL | Overlapping portion; NULL if disjoint |
| `btic_span(a, b)` | Btic | Smallest interval covering both inputs |
| `btic_gap(a, b)` | Btic or NULL | Gap between disjoint intervals; NULL if overlapping |

Set operations inherit metadata intelligently: when combining bounds, the finer granularity and least-certain certainty are preserved.

```cypher
// What period do these two events share?
RETURN btic_intersection(btic('1985'), btic('1985-06/1986-06')) AS overlap
// → [1985-06-01, 1986-01-01)

// What's the total span?
RETURN btic_span(btic('1985'), btic('1990')) AS span
// → [1985-01-01, 1991-01-01)

// What's the gap between them?
RETURN btic_gap(btic('1985'), btic('1990')) AS gap
// → [1986-01-01, 1990-01-01)
```

---

## Aggregation

| Function | Returns | Description |
|----------|---------|-------------|
| `btic_min(collection)` | Btic | Earliest interval by total order |
| `btic_max(collection)` | Btic | Latest interval by total order |
| `btic_span_agg(collection)` | Btic | Bounding interval of all inputs |
| `btic_count_at(collection, point)` | Int64 | Count of intervals containing the point |

```cypher
MATCH (e:Event)
RETURN btic_min(e.period) AS earliest,
       btic_max(e.period) AS latest,
       btic_span_agg(e.period) AS total_span

// How many events were active at a specific point?
MATCH (e:Event)
RETURN btic_count_at(e.period, 489024000000) AS active_count
```

---

## Python API

The `Btic` class is available directly in the Python bindings:

```python
from uni_db import Btic, Value

# Construction
b = Btic("1985")
b = Btic("1939/1945")
b = Btic("~1985")

# Properties
b.lo                # int — lower bound (ms since epoch)
b.hi                # int — upper bound (ms since epoch)
b.lo_granularity    # str — "year", "month", "day", ...
b.hi_granularity    # str
b.lo_certainty      # str — "definite", "approximate", ...
b.hi_certainty      # str
b.duration_ms       # int or None — duration in milliseconds
b.is_finite         # bool
b.is_unbounded      # bool

# Predicates (return bool)
b.overlaps(other)
b.contains(other)
b.before(other)
b.after(other)
b.meets(other)
b.contains_point(ms_since_epoch)

# Set operations (return Btic or None)
b.intersection(other)
b.span(other)
b.gap(other)

# Comparison (Btic is fully ordered)
Btic("1985") < Btic("2000")  # True
```

### Round-trip Example

```python
from uni_db import Uni, DataType, Btic

db = Uni.temporary()
db.schema().label("Event") \
    .property("name", DataType.STRING()) \
    .property("period", DataType.BTIC()) \
    .apply()

session = db.session()
with session.tx() as tx:
    tx.execute(
        "CREATE (e:Event {name: $name, period: $p})",
        params={"name": "Renaissance", "p": Btic("1400/1600")}
    )
    tx.commit()

result = session.query(
    "MATCH (e:Event) WHERE btic_overlaps(e.period, btic('1500')) RETURN e.name"
)
print(result.rows[0]["e.name"])  # "Renaissance"
```

---

## When to Use BTIC vs uni.temporal.validAt

| Scenario | Recommended | Why |
|----------|-------------|-----|
| Exact date ranges with two columns (`start_date`, `end_date`) | `uni.temporal.validAt` | Simpler; handles NULL end dates natively |
| Single-column fuzzy intervals | BTIC | Granularity metadata preserved |
| Historical records with variable precision (year, month, day) | BTIC | Remembers "this was a year" vs "365 days" |
| Approximate or uncertain dates | BTIC | Certainty annotations (`~`, `?`, `??`) |
| Allen's interval algebra (overlaps, meets, during, ...) | BTIC | 12 built-in predicates |
| Simple "is this record valid now?" checks | `uni.temporal.validAt` | One-liner, optimized for common case |
| Bitemporal queries (valid time + transaction time) | BTIC + `VERSION AS OF` | BTIC handles valid time; MVCC handles transaction time |

---

## Granularity Reference

BTIC supports 11 granularity levels, inferred automatically from the literal precision:

| Literal | Granularity | Interval width |
|---------|-------------|----------------|
| `'1985'` | Year | 1 year |
| `'1985-03'` | Month | 1 month |
| `'1985-03-15'` | Day | 1 day |
| `'1985-03-15T14'` | Hour | 1 hour |
| `'1985-03-15T14:30'` | Minute | 1 minute |
| `'1985-03-15T14:30:00'` | Second | 1 second |
| `'1985-03-15T14:30:00.000Z'` | Millisecond | 1 millisecond |

Higher granularities (decade, century, millennium) are available in the codec but require explicit construction via the raw API.

---

## Certainty Reference

Each bound independently carries a certainty level:

| Certainty | Prefix | Meaning |
|-----------|--------|---------|
| Definite | *(none)* | Known precisely to stated granularity |
| Approximate | `~` | Estimated; true value may differ by +/-1 granularity unit |
| Uncertain | `?` | Weakly estimated; could differ significantly |
| Unknown | `??` | Not meaningfully known; stored value is a best guess |

Certainty is advisory metadata — it does not affect comparisons or predicate evaluation, but downstream consumers can use it for probabilistic reasoning or display.
