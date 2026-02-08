# TCK Test Runner

The Uni TCK runner is built on the `cucumber-rs` crate and supports running specific feature sets via the `TCK_FEATURE` environment variable.

## Running Tests

### Basic Usage (Run All)
```bash
cargo test --package uni-tck --test cucumber
```

### Running Specific Features
Set the `TCK_FEATURE` environment variable to a feature directory path or a shortcut keyword.

**Syntax:**
```bash
TCK_FEATURE=<feature_filter> cargo test --package uni-tck --test cucumber
```

**Supported Shortcuts:**
- `boolean` -> `tck/features/expressions/boolean`
- `comparison` -> `tck/features/expressions/comparison`
- `match` -> `tck/features/clauses/match`

**Custom Path:**
You can also provide a relative path:
```bash
TCK_FEATURE=tck/features/expressions/null cargo test --package uni-tck --test cucumber
```

## Parallel Execution
Since the runner is a single binary, "parallelism" here refers to:
1.  **Internal Parallelism:** `cucumber-rs` runs scenarios concurrently if configured (Uni runs sequentially by default for safety).
2.  **Process Parallelism:** You can run multiple instances of the test runner targeting different features.

**Example: Running Comparison and Boolean tests in parallel**
```bash
TCK_FEATURE=comparison cargo test --package uni-tck --test cucumber &
TCK_FEATURE=boolean cargo test --package uni-tck --test cucumber &
wait
```

## Reports
Results are written to `target/cucumber/results.json`. Note that running multiple instances concurrently will overwrite this file unless you configure separate output directories (currently not supported).