// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Embedding service implementations for vector search.
//!
//! This module provides text embedding services using different backends:
//!
//! - **Candle** (default): Native Rust ML framework, no ONNX Runtime dependency
//! - **FastEmbed** (optional): ONNX-based, requires `fastembed` feature flag
//!
//! # Feature Flags
//!
//! - `candle-text`: Enables Candle-based text embeddings (default)
//! - `candle-cuda`: Enables CUDA GPU acceleration for Candle
//! - `fastembed`: Enables legacy ONNX-based FastEmbed embeddings

mod service;

#[cfg(feature = "candle-text")]
mod candle_text;

// Re-export the EmbeddingService trait, factory, and helpers (always available)
pub use service::{EmbeddingService, create_embedding_service, embedding_service_key};

// Candle exports (default)
#[cfg(feature = "candle-text")]
pub use candle_text::{CandleTextEmbedding, CandleTextModel};

// FastEmbed exports (optional)
#[cfg(feature = "fastembed")]
pub use service::FastEmbedService;
