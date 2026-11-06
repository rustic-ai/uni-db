# uni-db-metal

Python bindings for Uni Graph Database with **all 11 providers** (candle and ONNX local, 8 hosted APIs, and the llama.cpp server adapter) and **Apple GPU/ANE acceleration**: candle runs on Metal, and the ONNX runtime runs on the CoreML execution provider, falling back to CPU at load for models CoreML cannot run.

This is the recommended wheel for Apple Silicon Macs.

```
pip install uni-db-metal
```

For the full wheel matrix (slim, CPU, NVIDIA CUDA variants), see the [migration guide](https://github.com/rustic-ai/uni-db/blob/main/docs/migrations/0.9.0-wheel-matrix-collapse.md). For full documentation, see https://rustic-ai.github.io/uni-db.

The Python API is identical across all `uni-db` wheel variants — pick the wheel that matches the providers and accelerator your deployment needs.
