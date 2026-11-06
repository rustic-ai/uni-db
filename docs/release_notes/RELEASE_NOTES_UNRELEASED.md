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

**Python: a malformed auto-embedding config now raises instead of being
dropped.** `LabelBuilder.index(prop, {"type": "vector", "embedding": {...}})`
parsed the `embedding` dict with an `Option` chain, so any mistake removed it
without a word. The common case was `"source"`, the key Cypher DDL uses: the
index was created without auto-embedding, and the first insert failed with
`Property 'embedding' cannot be null`, which never mentions the config. The
binding now mirrors the Cypher parser:

- `"source"` is accepted alongside `"source_properties"`; giving both is an
  error.
- `alias` and a non-empty source list are required, unknown keys are rejected
  with the accepted list, and `batch_size` must be a positive integer.
- The default `batch_size` is 32, matching Cypher DDL, instead of 100.

Each of these raises `ValueError` from `apply()`. A config that was being
silently ignored before will now fail there instead; with a typo-free config,
nothing changes apart from the batch-size default.

## Correctness fixes

- **`size()` and `length()` of a vector return its dimension count.** A
  stored `VECTOR(n)` value is read as an Arrow `FixedSizeList`, which the
  columnar `size()` did not handle, so `size(d.embedding)` failed, and its error
  message printed every element of the vector. The row-at-a-time evaluator had
  the same gap for both functions. Both paths now return `n` (null for a null
  vector), and an unsupported argument raises a `TypeError` naming the type
  rather than the value.
- **Type errors raised during a query no longer end in a dangling "got".**
  The query layer wraps an engine `TypeError:` message in `UniError::Type`
  with the whole message as `expected` and an empty `actual`, which displayed
  as `Type error: expected <message>, got ` (in Python, `UniTypeError` carries
  the same text). That shape now displays as `Type error: <message>`; type
  errors that name both types are unchanged. The TCK matcher reads detail codes
  from both shapes.

## Dependencies and build requirements

- uni-xervo floor raised to **0.19.1**. 0.19.0 must not be admitted: its ONNX
  Runtime let CoreML claim nodes on CPU-only macOS builds (see above).
- candle 0.10 → 0.11.1, hf-hub 0.5 → 1.0, ort 2.0.0-rc.12 → rc.15 (ONNX
  Runtime 1.24 → 1.31), all through uni-xervo.
- zip 7.2 → 9.4: RUSTSEC-2026-0367, -0368 and -0369 no longer apply, and
  their `deny.toml` ignores are removed.
- The unused `candle-core`, `candle-nn`, `candle-transformers`, `hf-hub` and
  `tokenizers` entries are removed from `[workspace.dependencies]`.
