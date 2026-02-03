#!/usr/bin/env python3
"""Validate Rust Jupyter notebooks for correct structure.

Note: Actually running Rust notebooks requires evcxr_jupyter kernel.
This script validates the notebook structure and syntax.
"""

import glob
import json
import os
import sys


def validate_notebook(notebook_path):
    """Validate a single notebook."""
    print(f"Validating {os.path.basename(notebook_path)}...")

    try:
        with open(notebook_path) as f:
            nb = json.load(f)
    except json.JSONDecodeError as e:
        print(f"  ERROR: Invalid JSON - {e}")
        return False

    # Check required fields
    if "cells" not in nb:
        print("  ERROR: Missing 'cells' field")
        return False

    if "metadata" not in nb:
        print("  ERROR: Missing 'metadata' field")
        return False

    # Check kernel spec
    kernelspec = nb.get("metadata", {}).get("kernelspec", {})
    if kernelspec.get("name") != "rust":
        print(f"  WARNING: Kernel is '{kernelspec.get('name')}', expected 'rust'")

    # Count cells
    code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
    md_cells = [c for c in nb["cells"] if c["cell_type"] == "markdown"]

    print(f"  Found {len(code_cells)} code cells, {len(md_cells)} markdown cells")

    # Validate code cells have basic Rust syntax indicators
    has_dep = False
    has_use = False
    has_run_macro = False

    for cell in code_cells:
        source = cell.get("source", "")
        if isinstance(source, list):
            source = "".join(source)

        if ":dep " in source:
            has_dep = True
        if "use uni::" in source or "use std::" in source:
            has_use = True
        if "run!(" in source:
            has_run_macro = True

    if not has_dep:
        print("  WARNING: No :dep statements found (expected for evcxr)")
    if not has_use:
        print("  WARNING: No 'use' statements found")
    if not has_run_macro:
        print("  WARNING: No run!() macro found (async code may not work)")

    print("  OK")
    return True


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    notebooks = glob.glob(os.path.join(base_dir, "*.ipynb"))

    if not notebooks:
        print("No notebooks found!")
        sys.exit(1)

    print(f"Found {len(notebooks)} notebooks\n")

    success = True
    for nb in sorted(notebooks):
        if not validate_notebook(nb):
            success = False

    print()
    if success:
        print("All notebooks validated successfully!")
        sys.exit(0)
    else:
        print("Some notebooks have issues.")
        sys.exit(1)


if __name__ == "__main__":
    main()
