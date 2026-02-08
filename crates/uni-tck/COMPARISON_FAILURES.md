# TCK Comparison Failures Analysis

## Summary
- **Total Scenarios**: 72
- **Passed**: 37
- **Failed**: 35

## Key Failure Categories

### 1. Heterogeneous Type Comparisons
- **Symptoms**: Panics with `Arrays with inconsistent types passed to MutableArrayData`.
- **Cause**: DataFusion strict typing vs Cypher's loose/total ordering.
- **Example**: `Comparing lists to lists` (Int64 vs Null or String).
- **TCK Reference**: `Comparison2 - Half-bounded Range` -> `Comparing across types yields null, except numbers`.

### 2. NaN Handling
- **Symptoms**: Incorrect Boolean results (Null instead of False/True) or mismatch.
- **Cause**: IEEE 754 vs Cypher NaN rules.
- **Example**: `Equality and inequality of NaN`.

### 3. Range Comparisons
- **Symptoms**: Row count mismatches (returning 0 instead of expected rows).
- **Cause**: Likely issue in translating chained comparisons (`a < x < b`) or simple range logic.
- **TCK Reference**: `Comparison3 - Full-Bound Range`.

### 4. Large Integers
- **Symptoms**: Row count mismatch.
- **Cause**: Precision loss or overflow handling.
- **Example**: `Handling inlined equality of large integer`.
