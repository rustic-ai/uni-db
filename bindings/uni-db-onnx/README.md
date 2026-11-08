# uni-db-onnx

Python bindings for Uni Graph Database — **slim variant**: the unified xervo `local/onnx` provider (raw, rerank, embed) plus the 8 hosted-API providers and the llama.cpp server adapter, **without** candle. Smaller wheel, faster cold start; suitable when local LLM inference isn't needed.

ORT is statically linked (CPU); no `ORT_DYLIB_PATH` setup required.

```
pip install uni-db-onnx
```

For the full wheel matrix (default-everything, CUDA, Apple GPU/ANE), see the [migration guide](https://github.com/rustic-ai/uni-db/blob/main/docs/migrations/0.9.0-wheel-matrix-collapse.md). For full documentation, see https://rustic-ai.github.io/uni-db.

The Python API is identical across all `uni-db` wheel variants — pick the wheel that matches the providers and accelerator your deployment needs.
