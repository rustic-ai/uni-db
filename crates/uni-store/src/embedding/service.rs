// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use fastembed::{InitOptions, TextEmbedding};
use std::sync::{Arc, Mutex};

#[async_trait]
pub trait EmbeddingService: Send + Sync {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>>;
    fn dimensions(&self) -> u32;
    fn model_name(&self) -> &str;
}

pub struct FastEmbedService {
    model: Arc<Mutex<TextEmbedding>>,
    model_name: String,
    dimensions: u32,
}

impl FastEmbedService {
    pub fn new(model_name: &str, cache_dir: Option<&str>) -> Result<Self> {
        let model_enum = match model_name {
            "AllMiniLML6V2" => fastembed::EmbeddingModel::AllMiniLML6V2,
            "BGEBaseENV15" => fastembed::EmbeddingModel::BGEBaseENV15,
            "BGESmallENV15" => fastembed::EmbeddingModel::BGESmallENV15,
            "NomicEmbedTextV15" => fastembed::EmbeddingModel::NomicEmbedTextV15,
            "MultilingualE5Small" => fastembed::EmbeddingModel::MultilingualE5Small,
            _ => {
                return Err(anyhow!(
                    "Unsupported FastEmbed model: {}. Supported: AllMiniLML6V2, BGEBaseENV15, BGESmallENV15, NomicEmbedTextV15, MultilingualE5Small",
                    model_name
                ));
            }
        };

        let mut options = InitOptions::new(model_enum);
        if let Some(dir) = cache_dir {
            options = options.with_cache_dir(std::path::PathBuf::from(dir));
        }

        let model = TextEmbedding::try_new(options)
            .map_err(|e| anyhow!("Failed to initialize FastEmbed model: {}", e))?;

        // Determine dimensions based on known models to avoid runtime check
        let dimensions = match model_name {
            "AllMiniLML6V2" => 384,
            "BGESmallENV15" => 384,
            "MultilingualE5Small" => 384,
            "BGEBaseENV15" => 768,
            "NomicEmbedTextV15" => 768,
            _ => 384,
        };

        Ok(Self {
            model: Arc::new(Mutex::new(model)),
            model_name: model_name.to_string(),
            dimensions,
        })
    }
}

#[async_trait]
impl EmbeddingService for FastEmbedService {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let texts_vec: Vec<String> = texts.iter().map(|s| s.to_string()).collect();
        let model = self.model.clone();

        tokio::task::spawn_blocking(move || {
            let mut guard = model
                .lock()
                .map_err(|_| anyhow!("Failed to lock embedding model"))?;
            guard
                .embed(texts_vec, None)
                .map_err(|e| anyhow!("FastEmbed error: {}", e))
        })
        .await?
    }

    fn dimensions(&self) -> u32 {
        self.dimensions
    }

    fn model_name(&self) -> &str {
        &self.model_name
    }
}
