# uni-db — unreleased

Changes since [5.0.0](RELEASE_NOTES_5.0.0.md) (released 2026-10-28).

## ⚠️ Behaviour and API changes

**The `local/mistralrs` provider is removed (breaking for Rust feature
users).** uni-xervo 0.19 dropped it upstream, so the `provider-mistralrs`
feature is gone from `uni-db` and `uni-cli`, and a `Cargo.toml` that names it
no longer resolves. A catalog alias with `provider_id: "local/mistralrs"` now
fails to load. Replacements:

| Capability | Use instead |
|---|---|
| local text / vision generation | `remote/llamacpp` against a `llama-server` you run |
| local embeddings | `local/onnx` or `local/candle` |
| image generation, speech synthesis | none |

The Python wheels need no code change: `uni-db`, `uni-db-cuda` and
`uni-db-metal` simply stop bundling mistral.rs, which makes them smaller and
faster to build. The default wheels now carry 11 providers and the slim
`-onnx` wheels 10.

**ONNX models on GPU wheels fall back to CPU when the accelerator cannot run
them.** With uni-xervo 0.19.1, an ONNX model is probed with one small
inference at load. If the accelerator fails it and the alias's
`execution_providers` list includes `"cpu"` (the default lists do), the model
is reloaded on CPU with a warning; a list without `"cpu"`, such as
`["coreml"]`, makes it a load error rather than a failure on the first query.
CPU-only builds are not probed. This fixes Qwen3-Reranker and
Qwen3-Embedding on Apple Silicon, which failed on every query under
uni-xervo 0.19.0, on the CPU-only wheels as well as `uni-db-metal`.

## Dependencies and build requirements

- uni-xervo floor raised to **0.19.1**. 0.19.0 must not be admitted: its ONNX
  Runtime let CoreML claim nodes on CPU-only macOS builds (see above).
- candle 0.10 → 0.11.1, hf-hub 0.5 → 1.0, ort 2.0.0-rc.12 → rc.15 (ONNX
  Runtime 1.24 → 1.31), all through uni-xervo.
- zip 7.2 → 9.4: RUSTSEC-2026-0367, -0368 and -0369 no longer apply, and
  their `deny.toml` ignores are removed.
- The unused `candle-core`, `candle-nn`, `candle-transformers`, `hf-hub` and
  `tokenizers` entries are removed from `[workspace.dependencies]`.
