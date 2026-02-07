# Rust Style Guide

## General Principles
- **Consistency:** Follow standard Rust community conventions.
- **Automation:** Use `rustfmt` and `clippy` to enforce style automatically.
- **Safety:** Prioritize safe code over `unsafe` blocks. Document safety invariants for all `unsafe` code.

## Formatting (Enforced by `rustfmt`)
- **Indentation:** 4 spaces (no tabs).
- **Line Width:** 100 characters.
- **Block Indent:** Prefer block indentation over visual alignment.
- **Trailing Commas:** Use trailing commas in multi-line lists (structs, matches, function args).
- **Imports:** Group imports by crate (`std`, external, internal). Use `crate::` or `super::` for internal imports.

## Naming Conventions
- **Types (Structs, Enums, Traits):** `UpperCamelCase`
- **Functions, Methods, Variables, Modules:** `snake_case`
- **Constants, Statics:** `SCREAMING_SNAKE_CASE`
- **Lifetimes:** `'short_lowercase` (e.g., `'a`, `'ctx`)
- **Getters:** `field_name()` (not `get_field_name()`)
- **Conversions:** `as_target_type()` (cheap), `to_target_type()` (expensive)

## Idiomatic Patterns

### Error Handling
- Use `Result<T, E>` for recoverable errors.
- Use `anyhow::Result` for application code, `thiserror` for libraries.
- Prefer `?` operator over `match` for error propagation.
- **Panic:** Use `panic!` only for unrecoverable state or contract violations (bugs).

### Ownership & Borrowing
- Prefer borrowing (`&T`) over cloning (`T.clone()`) where possible.
- Use `Cow<T>` for data that might be owned or borrowed.
- Avoid `Rc<RefCell<T>>` unless shared mutability is strictly required; prefer channel-based concurrency.

### Documentation
- **Public API:** All public items must have `///` doc comments.
- **Module Docs:** Use `//!` at the top of the file.
- **Examples:** Include doctests in documentation.

## Testing
- **Unit Tests:** Colocate with source code in a `tests` module at the bottom of the file.
- **Integration Tests:** Place in `tests/` directory at the crate root.
- **Property Testing:** Use `proptest` for fuzzing inputs.
