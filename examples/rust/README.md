# Uni Rust Jupyter Notebooks

Interactive Rust examples for Uni graph database using Jupyter notebooks with the `evcxr_jupyter` kernel.

## Prerequisites

1. **Rust toolchain** (1.70+)
2. **Jupyter** (notebook or lab)
3. **evcxr_jupyter** - Rust kernel for Jupyter

## Installation

### 1. Install Jupyter (if not already installed)

```bash
# Using pip
pip install jupyter

# Or using conda
conda install jupyter
```

### 2. Install evcxr_jupyter

```bash
# Install the Rust Jupyter kernel
cargo install evcxr_jupyter

# Register the kernel with Jupyter
evcxr_jupyter --install
```

### 3. Verify installation

```bash
jupyter kernelspec list
# Should show 'rust' in the list
```

## Running the Notebooks

```bash
# From this directory
jupyter notebook

# Or with JupyterLab
jupyter lab
```

Then open any `.ipynb` file and ensure the kernel is set to "Rust".

## Available Notebooks

| Notebook | Description |
|----------|-------------|
| `supply_chain.ipynb` | BOM explosion and cost rollup for supply chain management |
| `recommendation.ipynb` | Collaborative filtering and vector similarity recommendations |
| `rag.ipynb` | Retrieval-Augmented Generation with knowledge graph + vectors |
| `fraud_detection.ipynb` | Cycle detection and shared device analysis for fraud |
| `sales_analytics.ipynb` | Graph traversal with columnar aggregations |

## Notebook Structure

Each notebook demonstrates the core Uni Rust API:

```rust
// Load dependencies (evcxr special syntax)
:dep uni-db = { path = "../../../crates/uni" }
:dep tokio = { version = "1", features = ["full"] }

// Imports
use uni_db::{Uni, DataType};

// Async helper macro
macro_rules! run {
    ($e:expr) => {
        tokio::runtime::Runtime::new().unwrap().block_on($e)
    };
}

// Open database
let db = run!(Uni::open("./my_db").build()).unwrap();

// Define schema
run!(db.schema()
    .label("Person")
        .property("name", DataType::String)
    .apply()
).unwrap();

// Query
let results = run!(db.query("MATCH (n:Person) RETURN n.name")).unwrap();
```

## Regenerating Notebooks

If you modify the generator script:

```bash
python3 generate_notebooks.py
```

## Troubleshooting

### "Rust kernel not found"
Ensure evcxr_jupyter is installed and registered:
```bash
evcxr_jupyter --install
```

### Compilation errors in cells
- Check that the `uni` crate path is correct (relative to notebook location)
- Ensure all dependencies are specified with `:dep`

### Slow first cell execution
The first cell in each notebook compiles the dependencies, which takes time. Subsequent cells are faster.

## Differences from Python Notebooks

| Feature | Python | Rust |
|---------|--------|------|
| Async handling | Automatic (PyO3 handles it) | Manual with `tokio::runtime::block_on` |
| Dependencies | `import uni` | `:dep uni-db = { path = "..." }` |
| Schema API | `db.create_label()` | `db.schema().label().apply()` |
| Error handling | Exceptions | `Result<T, E>` with `.unwrap()` |

## See Also

- [Python examples](../../bindings/uni-db/examples/) - Same use cases with Python bindings
- [Uni documentation](../../docs/) - Full API documentation
