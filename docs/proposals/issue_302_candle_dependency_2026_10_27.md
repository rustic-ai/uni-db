# Issue #302: remove Locy's unconditional Candle dependency

The local implementation addresses [#302](https://github.com/rustic-ai/uni-db/issues/302)
without introducing a new feature or removing the public classifier. This report
describes local source and validation, not merge or release status.

## Implementation and compatibility

`uni-locy` now depends directly on safetensors 0.7, already present in the
workspace dependency graph, instead of candle-core and candle-nn. The historical
`CandleLinearClassifier` type and its `load` method, trait implementation,
registration, and debug name remain available.

The loader reads validated safetensors data and decodes little-endian F32 weights
and bias without alignment assumptions. As before, tensors are flattened: weight
element count must match the feature order, and bias must contain exactly one
value. Missing files/tensors, invalid files, incompatible dtypes, and element-count
mismatches return `ClassifierError::Provider`. Diagnostic text now identifies the
linear loader rather than Candle.

Inference computes the ordered dot product, adds bias, and applies sigmoid in
f32, widening the resulting probabilities to f64. Existing float, integer,
boolean, hashed-string, null, missing, and vector feature encoding is preserved.
The default `classify_logits` inverse-sigmoid behavior is unchanged. Scalar
arithmetic may differ from the previous matrix kernel by f32 rounding; this is
not a promise of bitwise identity across hardware or math libraries.

The Locy TCK fixture writer also uses safetensors directly, eliminating its own
Candle dependency. Historical scenario names and Given syntax remain supported.
Optional Xervo embedding providers can still bring Candle into other builds.

## Dependency footprint

The same normal-dependency command was captured before and after the change:

```sh
cargo tree -p uni-db --no-default-features --features lance-backend \
  -e normal --prefix none --format '{p}' --offline
```

After normalizing repeated `(*)` lines, unique package/version entries drop from
**629 to 585**, removing **44 packages** and adding none. The removed set includes
candle-core/candle-nn, tokenizers 0.22, onig/onig_sys, the Candle gemm 0.19 family,
and their exclusive support dependencies. Safetensors was already reachable.
See the [exact dependency comparison](issue_302_candle_dependency_2026_10_27/dependency-comparison.json)
and compressed before/after graphs. This is a dependency-graph measurement;
cold-build time, peak memory, artifact size, and wheel-size savings have not
been measured.

## Validation

- Captured a nine-row reference from the previous Candle CPU implementation,
  covering mixed feature types and logits from large negative to large positive.
  The replacement matches that reference within f32 rounding tolerance, including
  tiny probabilities and saturated zero/one outputs.
- All **239 uni-locy tests passed**, including new model-file, dtype, element-count,
  flattened-shape, bias-only, empty-batch, and prediction-parity coverage.
- All **four classifier TCK scenarios passed in each schema mode**, using
  `scripts/run_locy_tck_with_report.sh --both '~CandleClassifier'`.
- All eight static guards passed. The separate fuzz workspace's locked offline
  metadata check passed without lockfile changes; uni-locy is not in that graph.
- Formatting passed. Strict Clippy passed for all targets of uni-locy and
  uni-locy-tck (`--no-deps -- -D warnings`).

Saved logs and source hashes are recorded alongside this report. These checks
target the classifier and dependency change; they do not constitute a full
workspace or cross-platform CI rerun.
