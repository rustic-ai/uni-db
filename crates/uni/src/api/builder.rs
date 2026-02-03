// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::Value;
use std::collections::HashMap;

/// A fluent builder for constructing property maps.
#[derive(Debug, Default, Clone)]
pub struct PropertiesBuilder {
    properties: HashMap<String, Value>,
}

impl PropertiesBuilder {
    /// Create a new empty properties builder.
    pub fn new() -> Self {
        Self {
            properties: HashMap::new(),
        }
    }

    /// Add a property to the builder.
    /// The value can be anything that converts into `serde_json::Value`.
    pub fn add(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Build the properties map.
    pub fn build(self) -> HashMap<String, Value> {
        self.properties
    }
}
