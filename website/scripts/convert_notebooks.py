#!/usr/bin/env python3
"""Execute and convert Jupyter notebooks to Markdown for Zensical documentation.

Zensical does not support the mkdocs-jupyter plugin, so we pre-execute
and convert all .ipynb files to .md. This replaces the old mkdocs-jupyter
``execute: true`` behaviour.

Each notebook is executed in a **separate subprocess** to avoid conflicts
between the Jupyter kernel's event loop and native extensions (e.g.
uni_db's tokio runtime + bundled OpenSSL vs. zmq's OpenSSL in the parent).

Rust notebooks (examples/rust/) are skipped — they require the evcxr
kernel which is not available in CI.

Usage:
    python website/scripts/convert_notebooks.py               # execute + convert
    python website/scripts/convert_notebooks.py --no-execute  # convert only

Run this before ``zensical build`` or ``zensical serve``. The generated
``.md`` files are gitignored; nav entries and ``*_overview.md`` pages link to
them, so a build without this step reports every such link as missing.

``--no-execute`` converts the committed (output-free) notebooks without
running them. It needs only ``nbconvert`` -- no built ``uni_db`` wheel and no
Jupyter kernel -- and leaves the tracked ``.ipynb`` files untouched, which is
what the local docs check uses. Executing writes cell outputs back into the
``.ipynb`` files; the deploy workflow does that on a throwaway checkout.
"""

import argparse
import subprocess
import sys
from pathlib import Path

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"

# Directories whose notebooks should NOT be executed (no kernel available).
# They are still converted to Markdown, just without execution.
EXECUTE_IGNORE = {"rust"}

EXECUTE_TIMEOUT = 120  # seconds per cell


def find_notebooks(docs_dir: Path) -> list[Path]:
    """Find all .ipynb files under the docs directory."""
    return sorted(docs_dir.rglob("*.ipynb"))


def should_execute(notebook_path: Path) -> bool:
    """Return True if this notebook should be executed before conversion."""
    return not any(part in EXECUTE_IGNORE for part in notebook_path.parts)


def execute_notebook(notebook_path: Path) -> bool:
    """Execute a notebook in a subprocess, writing outputs back into the .ipynb."""
    script = f"""\
import nbformat
from nbclient import NotebookClient

nb = nbformat.read({str(notebook_path)!r}, as_version=4)
client = NotebookClient(nb, timeout={EXECUTE_TIMEOUT}, kernel_name="python3")
client.execute()
nbformat.write(nb, {str(notebook_path)!r})
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=EXECUTE_TIMEOUT * 15,  # generous overall timeout
    )
    if result.returncode != 0:
        stderr = result.stderr.strip().splitlines()
        # Show last 3 lines of error for context
        err_summary = "\n    ".join(stderr[-3:]) if stderr else "unknown error"
        print(f"  EXEC ERROR:\n    {err_summary}")
        return False
    return True


def convert_to_markdown(notebook_path: Path) -> Path | None:
    """Convert an .ipynb (which may already contain outputs) to .md."""
    output_md = notebook_path.with_suffix(".md")

    cmd = [
        sys.executable,
        "-m",
        "nbconvert",
        "--to",
        "markdown",
        "--output",
        output_md.name,
        str(notebook_path),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  CONVERT ERROR: {result.stderr.strip()}")
        return None

    return output_md


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--no-execute",
        action="store_true",
        help="convert notebooks without executing them (no kernel or uni_db needed)",
    )
    args = parser.parse_args()

    notebooks = find_notebooks(DOCS_DIR)
    if not notebooks:
        print("No notebooks found.")
        return

    print(f"Converting {len(notebooks)} notebooks to markdown...")

    converted = 0
    failed = 0
    for nb_path in notebooks:
        rel = nb_path.relative_to(DOCS_DIR)
        execute = not args.no_execute and should_execute(nb_path)
        tag = "execute+convert" if execute else "convert only"
        print(f"  {rel} ({tag}) ", end="", flush=True)

        try:
            if execute:
                if not execute_notebook(nb_path):
                    failed += 1
                    continue
            out = convert_to_markdown(nb_path)
            if out:
                print(f"-> {out.name}")
                converted += 1
            else:
                failed += 1
        except Exception as exc:
            print(f"  ERROR: {exc}")
            failed += 1

    print(f"\nDone: {converted} converted, {failed} failed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
