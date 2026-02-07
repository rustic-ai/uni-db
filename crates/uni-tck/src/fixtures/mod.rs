use anyhow::Result;
use std::sync::OnceLock;
use uni_db::Uni;

mod graph_loader;
use graph_loader::GraphLoader;

static LOADER: OnceLock<GraphLoader> = OnceLock::new();

/// Load a named graph fixture into the database
pub async fn load_graph(db: &Uni, name: &str) -> Result<()> {
    LOADER.get_or_init(GraphLoader::new).load(db, name).await
}
