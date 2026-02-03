use anyhow::Result;
use uni_db::Uni;

mod binary_tree;

/// Load a named graph fixture into the database
pub async fn load_graph(db: &Uni, name: &str) -> Result<()> {
    match name {
        "binary-tree-1" => binary_tree::load_binary_tree_1(db).await,
        "binary-tree-2" => binary_tree::load_binary_tree_2(db).await,
        _ => Err(anyhow::anyhow!("Unknown graph fixture: {}", name)),
    }
}
