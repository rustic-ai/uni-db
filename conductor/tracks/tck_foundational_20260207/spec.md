# Specification - TCK Foundational Compatibility

## Goal
Pass all TCK scenarios related to basic literals, type conversions, and simple comparison logic.

## Scope
- **Literals:** Decimal, Hexadecimal, Octal integers; Floats; Strings (escapes/unicode); nested Lists and Maps.
- **Type Conversions:** `toInteger()`, `toFloat()`, `toBoolean()`, `toString()`.
- **Logic & Comparison:** `AND`, `OR`, `XOR`, `NOT` semantics; Equality and Range comparisons across types.

## Success Criteria
- 100% pass rate for `Literals*` feature files.
- 100% pass rate for `TypeConversion*` feature files.
- 100% pass rate for `Boolean*` and `Comparison*` feature files.
- No regressions in existing `MATCH` or `CREATE` functionality.
