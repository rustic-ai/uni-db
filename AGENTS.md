# Repository Guidelines

## Project Structure & Module Organization
- `src/`: main library and binary. Key submodules: `core/` (IDs, schema, snapshots), `storage/` (Lance datasets, LSM deltas), `runtime/` (WAL, L0 buffer, gryf runtime), `query/` (Cypher parser, planner, executor).
- `tests/`: integration tests (`tests/*.rs`).
- `benches/`: Criterion benchmarks (`benches/micro_benchmarks.rs`).
- `docs/`: design and status docs like `DESIGN.md` and `docs/KNOWN_ISSUES.md`.
- `demos/`: runnable demo scripts and walkthroughs.

## Build, Test, and Development Commands
- `cargo build`: compile the project.
- `cargo run`: run the local binary.
- `cargo nextest run`: run all tests (parallel, preferred).
- `cargo nextest run -E 'test(<name>)'`: run a specific test by name.
- `cargo nextest run --run-ignored all`: include ignored/slow perf tests.
- `cargo bench`: run Criterion benchmarks.
- `cargo fmt`: format code.
- `cargo clippy`: lint for common issues.
- Use `cargo nextest` instead of `cargo test` for regular test runs.

## Coding Style & Naming Conventions
- Follow standard Rust style and format with `rustfmt`.
- `snake_case` for modules/functions, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Keep file names aligned with module paths (e.g., `src/runtime/wal.rs`).
- Add brief comments only when logic is not obvious.

## Testing Guidelines
- Unit tests live next to code in `src/**` with `#[cfg(test)] mod tests`.
- Integration tests live in `tests/` and often use `#[tokio::test]`.
- Benchmarks go in `benches/` using Criterion.
- If a test is flaky or sensitive to parallelism, document it in `docs/KNOWN_ISSUES.md`.
- For TCK compliance runs, use `scripts/run_tck_with_report.sh`.
- For filtered TCK subsets, use `scripts/run_tck_with_report.sh "~Match1"` (replace filter as needed).
- TCK run artifacts are written under `target/cucumber/` (results/report) and synced into `compliance_reports/` by mode.

## Commit & Pull Request Guidelines
- Use Conventional Commits as in history: `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`.
- PRs should include a short rationale, tests run, and links to related issues.
- Update `DESIGN.md` and `CYPHER_GAPS.md` when architecture or Cypher support changes.

## Agent Git Safety Rule
- Do not perform any git action unless the user has explicitly instructed it in the current conversation turn.
- This includes all git commands and git-related workflows (for example: `status`, `diff`, `add`, `commit`, `push`, `pull`, `checkout`, `reset`, `merge`, `rebase`, `tag`, `stash`, `cherry-pick`).
