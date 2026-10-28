#!/usr/bin/env python3
"""Compile Rust Locy notebook cells; --run also evaluates their local examples.

Rust notebooks are not executed by convert_notebooks.py (CI has no evcxr).
This verifier combines their code cells into one ordinary Cargo example, then
removes its temporary source even if compilation or execution fails. Examples
use temporary databases and mock classifiers; no model downloads are required.
"""

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def notebook_source() -> tuple[str, int]:
    paths = sorted((ROOT / "website/docs/examples/rust").glob("locy*.ipynb"))
    if not paths:
        raise ValueError("no Rust Locy notebooks found")
    modules = []
    for path in paths:
        notebook = json.loads(path.read_text())
        code = "\n\n".join(
            "".join(cell["source"])
            for cell in notebook["cells"]
            if cell["cell_type"] == "code"
        )
        modules.append(
            f"mod {path.stem} {{\npub async fn run() -> uni_db::Result<()> {{\n"
            f"{code}\nOk(())\n}}\n}}"
        )
    calls = "\n".join(f"{path.stem}::run().await?;" for path in paths)
    main = (
        f"#[tokio::main]\nasync fn main() -> uni_db::Result<()> {{\n{calls}\nOk(())\n}}"
    )
    return "\n".join(modules) + "\n" + main, len(paths)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="also execute the notebooks")
    args = parser.parse_args()
    source, count = notebook_source()
    fd, name = tempfile.mkstemp(
        prefix="doc_check_", suffix=".rs", dir=ROOT / "crates/uni/examples"
    )
    path = Path(name)
    try:
        with os.fdopen(fd, "w") as output:
            output.write(source)
        result = subprocess.run(
            [
                "cargo",
                "run" if args.run else "check",
                "-p",
                "uni-db",
                "--example",
                path.stem,
                "--offline",
                "--locked",
            ],
            cwd=ROOT,
            check=False,
        )
        if result.returncode == 0:
            print(
                f"Validated {count} Rust Locy notebooks ({'execution' if args.run else 'compilation'})."
            )
        return result.returncode
    finally:
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
