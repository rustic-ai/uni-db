use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::PathBuf;
use uni_db::Uni;

#[derive(Debug, Deserialize)]
struct GraphMetadata {
    scripts: Vec<String>,
}

pub struct GraphLoader {
    graphs_dir: PathBuf,
}

impl GraphLoader {
    pub fn new() -> Self {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        Self {
            graphs_dir: manifest_dir.join("tck/graphs"),
        }
    }

    pub async fn load(&self, db: &Uni, name: &str) -> Result<()> {
        let graph_dir = self.graphs_dir.join(name);

        // Read JSON metadata
        let metadata_path = graph_dir.join(format!("{}.json", name));
        let metadata: GraphMetadata =
            serde_json::from_str(&std::fs::read_to_string(&metadata_path).with_context(|| {
                format!("Failed to read graph metadata: {}", metadata_path.display())
            })?)
            .with_context(|| {
                format!(
                    "Failed to parse graph metadata: {}",
                    metadata_path.display()
                )
            })?;

        // Execute each Cypher script
        for script_name in &metadata.scripts {
            let cypher_path = graph_dir.join(format!("{}.cypher", script_name));
            let content = std::fs::read_to_string(&cypher_path).with_context(|| {
                format!("Failed to read Cypher script: {}", cypher_path.display())
            })?;

            // Split on semicolons, execute each statement
            for stmt in content.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                db.execute(stmt)
                    .await
                    .with_context(|| format!("Failed to execute Cypher statement: {}", stmt))?;
            }
        }

        Ok(())
    }
}
