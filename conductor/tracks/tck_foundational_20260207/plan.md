# Implementation Plan - TCK Foundational Compatibility

## Phase 1: Literals & Type System
- [x] Task: Fix Decimal, Hex, and Octal integer literal overflow and parsing edge cases. 84feece
    - [ ] Write Tests
    - [ ] Implement Fix
- [x] Task: Fix String literal escaping and Unicode support. 0747675
    - [ ] Write Tests
    - [ ] Implement Fix
- [ ] Task: Resolve nested List and Map literal evaluation failures in the vectorized engine.
    - [ ] Write Tests
    - [ ] Implement Fix
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Literals & Type System' (Protocol in workflow.md)

## Phase 2: Type Conversions & Comparisons
- [ ] Task: Standardize `toInteger()`, `toFloat()`, and `toBoolean()` behavior according to TCK.
    - [ ] Write Tests
    - [ ] Implement Fix
- [ ] Task: Fix cross-type comparison semantics (e.g., comparing Int to String).
    - [ ] Write Tests
    - [ ] Implement Fix
- [ ] Task: Fix `NOT` operator logic for null and non-boolean values.
    - [ ] Write Tests
    - [ ] Implement Fix
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Type Conversions & Comparisons' (Protocol in workflow.md)
