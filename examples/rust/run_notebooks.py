#!/usr/bin/env python3
"""Run Rust Jupyter notebooks by extracting code and compiling as a test.

This script extracts code from Rust notebooks, converts evcxr syntax to standard Rust,
and runs them as integration tests.
"""

import glob
import json
import os
import re
import subprocess
import sys
import tempfile
import shutil


def extract_rust_code(notebook_path):
    """Extract Rust code from notebook, converting evcxr syntax."""
    with open(notebook_path) as f:
        nb = json.load(f)

    code_cells = [cell for cell in nb["cells"] if cell["cell_type"] == "code"]

    code_parts = []
    for cell in code_cells:
        source = cell.get("source", "")
        if isinstance(source, list):
            source = "".join(source)

        # Skip :dep lines (we'll handle dependencies in Cargo.toml)
        lines = []
        for line in source.split("\n"):
            if line.strip().startswith(":dep"):
                continue
            lines.append(line)
        source = "\n".join(lines)

        if source.strip():
            code_parts.append(source)

    return "\n\n".join(code_parts)


def convert_to_test(notebook_name, code):
    """Convert notebook code to a Rust test function."""
    # The code uses a run!() macro - we need to wrap it in an async test
    # Replace the macro with direct block_on calls in a test context

    test_name = notebook_name.replace(".ipynb", "").replace("-", "_")

    test_code = f'''
#[tokio::test]
async fn test_{test_name}() {{
    use uni::{{Uni, DataType, IndexType, ScalarType, VectorMetric, VectorAlgo, VectorIndexCfg}};
    use std::collections::HashMap;
    use serde_json::json;

    // Helper function to run async code (replaces the macro)
    async fn run_test() -> Result<(), Box<dyn std::error::Error>> {{
        {indent_code(code, 8)}
        Ok(())
    }}

    run_test().await.expect("Test failed");
}}
'''
    return test_code


def indent_code(code, spaces):
    """Indent code by specified spaces."""
    prefix = " " * spaces
    lines = code.split("\n")
    return "\n".join(prefix + line if line.strip() else line for line in lines)


def create_test_crate(notebooks_dir, output_dir):
    """Create a test crate with all notebook tests."""

    # Create Cargo.toml
    cargo_toml = '''[package]
name = "notebook-tests"
version = "0.1.0"
edition = "2024"

[dependencies]
uni = { path = "../../crates/uni" }
tokio = { version = "1", features = ["full", "rt-multi-thread", "macros"] }
serde_json = "1"
'''

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "src"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "tests"), exist_ok=True)

    with open(os.path.join(output_dir, "Cargo.toml"), "w") as f:
        f.write(cargo_toml)

    # Create empty lib.rs
    with open(os.path.join(output_dir, "src", "lib.rs"), "w") as f:
        f.write("// Placeholder\n")

    # Process each notebook
    notebooks = glob.glob(os.path.join(notebooks_dir, "*.ipynb"))

    for nb_path in notebooks:
        nb_name = os.path.basename(nb_path)
        print(f"Processing {nb_name}...")

        code = extract_rust_code(nb_path)

        # Create individual test file
        test_name = nb_name.replace(".ipynb", "").replace("-", "_")
        test_file = os.path.join(output_dir, "tests", f"{test_name}.rs")

        # Write as a standalone test
        with open(test_file, "w") as f:
            f.write(generate_standalone_test(test_name, code))


def generate_standalone_test(test_name, notebook_code):
    """Generate a standalone test file from notebook code."""

    # We need to transform the notebook code:
    # 1. Remove run!() macro calls and make it all async
    # 2. Keep the logic intact

    # The notebook code has patterns like:
    # - run!(async { ... }).unwrap();
    # - run!(db.query(...)).unwrap();
    # - let x = run!(...).unwrap();

    # Transform run!(expr) to expr.await
    transformed = notebook_code

    # Pattern: run!(async { ... })
    # This needs to become just the inner async block content
    transformed = re.sub(
        r'run!\(async\s*\{([^}]*(?:\{[^}]*\}[^}]*)*)\}\)',
        r'async { \1 }.await',
        transformed,
        flags=re.DOTALL
    )

    # Pattern: run!(expr) -> expr.await
    # But be careful with nested parens
    # Simple version: run!(db.something(...))
    transformed = re.sub(
        r'run!\(([^)]+(?:\([^)]*\)[^)]*)*)\)',
        r'(\1).await',
        transformed
    )

    # Fix double .await.await
    transformed = transformed.replace('.await.await', '.await')

    # The db variable needs to persist across the test
    # Wrap everything in an async block

    return f'''// Auto-generated test from {test_name}.ipynb
use uni::{{Uni, DataType, IndexType, ScalarType, VectorMetric, VectorAlgo, VectorIndexCfg}};
use std::collections::HashMap;
use serde_json::json;

#[tokio::test]
async fn test_{test_name}() {{
    // Clean up any existing test database
    let db_path = "./{test_name}_test_db";
    if std::path::Path::new(db_path).exists() {{
        std::fs::remove_dir_all(db_path).unwrap();
    }}

    let db = Uni::open(db_path).build().await.unwrap();

{indent_code(transform_notebook_to_test(notebook_code, test_name), 4)}

    // Cleanup
    drop(db);
    if std::path::Path::new(db_path).exists() {{
        std::fs::remove_dir_all(db_path).ok();
    }}
}}
'''


def transform_notebook_to_test(code, test_name):
    """Transform notebook code to test code."""

    lines = code.split('\n')
    output_lines = []

    # Skip the imports and macro definition (we add them at the top)
    skip_until_db = True
    in_macro = False

    for line in lines:
        # Skip use statements (we add them at the top)
        if line.strip().startswith('use '):
            continue

        # Skip macro definition
        if 'macro_rules!' in line:
            in_macro = True
            continue
        if in_macro:
            if line.strip() == '}':
                in_macro = False
            continue

        # Skip db_path and db creation (we do it ourselves)
        if 'let db_path' in line or 'db_path).exists()' in line or 'remove_dir_all(db_path)' in line:
            continue
        if 'Uni::open(db_path)' in line:
            continue
        if 'println!("Opened database' in line:
            continue

        # Transform run!(async { ... }) blocks
        if 'run!(async {' in line:
            # Start of async block - just use the inner content with .await
            line = line.replace('run!(async {', '(async {')

        # Transform run!(...) to (...).await
        if 'run!(' in line and 'async' not in line:
            # Simple run! call
            line = re.sub(r'run!\(([^;]+)\)', r'(\1).await', line)

        # Close async blocks properly
        if '}).unwrap()' in line and 'async' not in line:
            line = line.replace('}).unwrap()', '}).await.unwrap()')

        output_lines.append(line)

    result = '\n'.join(output_lines)

    # Clean up any artifacts
    result = result.replace('.await.await', '.await')
    result = result.replace('((', '(')
    result = result.replace('))', ')')

    return result


def run_tests(test_dir):
    """Run the generated tests with cargo."""
    print("\nRunning Rust tests...")

    result = subprocess.run(
        ["cargo", "test", "--", "--nocapture"],
        cwd=test_dir,
        capture_output=True,
        text=True,
        timeout=600
    )

    print(result.stdout)
    if result.stderr:
        print(result.stderr)

    return result.returncode == 0


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    notebooks_dir = script_dir

    # Create test crate in a temp directory
    test_dir = os.path.join(script_dir, ".notebook_tests")

    print("Extracting notebook code...")
    create_test_crate(notebooks_dir, test_dir)

    print("\nGenerated test files:")
    for f in glob.glob(os.path.join(test_dir, "tests", "*.rs")):
        print(f"  {os.path.basename(f)}")

    success = run_tests(test_dir)

    # Cleanup
    # shutil.rmtree(test_dir, ignore_errors=True)

    if success:
        print("\nAll Rust notebook tests passed!")
        sys.exit(0)
    else:
        print("\nSome Rust notebook tests failed.")
        print(f"Test directory preserved at: {test_dir}")
        sys.exit(1)


if __name__ == "__main__":
    main()
