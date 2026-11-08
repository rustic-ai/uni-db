# uni-db-cuda

Python bindings for Uni Graph Database with **all 11 providers** (candle and ONNX local, 8 hosted APIs, and the llama.cpp server adapter) and **NVIDIA CUDA acceleration** for the local providers.

This is the recommended wheel for Linux/Windows hosts with an NVIDIA GPU.

```
pip install uni-db-cuda
```

For the full wheel matrix (slim, CPU, Apple GPU/ANE variants), see the [migration guide](https://github.com/rustic-ai/uni-db/blob/main/docs/migrations/0.9.0-wheel-matrix-collapse.md). For full documentation, see https://rustic-ai.github.io/uni-db.

The Python API is identical across all `uni-db` wheel variants — pick the wheel that matches the providers and accelerator your deployment needs.
