# Implementation Plan - TCK Foundational Compatibility

## Phase 1: Literals & Type System [checkpoint: f10a8a1]
- [x] Task: Fix Decimal, Hex, and Octal integer literal overflow and parsing edge cases. 84feece
    - [x] Write Tests
    - [x] Implement Fix
- [x] Task: Fix String literal escaping and Unicode support. 0747675
    - [x] Write Tests
    - [x] Implement Fix
- [x] Task: Resolve nested List and Map literal evaluation failures in the vectorized engine. 6448d33
    - [x] Write Tests
    - [x] Implement Fix
- [x] Task: Conductor - User Manual Verification 'Phase 1: Literals & Type System' (Protocol in workflow.md) f10a8a1

## Phase 2: Type Conversions & Comparisons [checkpoint: b0e570a]
- [x] Task: Standardize `toInteger()`, `toFloat()`, and `toBoolean()` behavior according to TCK.
    - [x] Write Tests
    - [x] Implement Fix
- [x] Task: Fix cross-type comparison semantics (e.g., comparing Int to String).
    - [x] Write Tests
    - [x] Implement Fix
- [x] Task: Fix `NOT` operator logic for null and non-boolean values.
    - [x] Write Tests
    - [x] Implement Fix
- [x] Task: Conductor - User Manual Verification 'Phase 2: Type Conversions & Comparisons' (Protocol in workflow.md) b0e570a
