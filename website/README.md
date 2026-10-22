# Uni Documentation

Documentation site for Uni, the embedded multi-model graph database.

## Setup

Run these from `website/`. The tools come from PyPI through `uv`; no
project virtualenv is needed.

```bash
# Generate the notebook pages (gitignored *.md next to each .ipynb).
# Nav entries and *_overview.md pages link to them, so skipping this step
# makes the build report every such link as a missing page.
uv run --no-project --with nbconvert python scripts/convert_notebooks.py --no-execute

# Build the static site; --strict fails the build on any warning
# (broken links included)
uv run --no-project --with zensical --with pymdown-extensions zensical build --strict

# Serve locally with hot reload
uv run --no-project --with zensical --with pymdown-extensions zensical serve
```

`--no-execute` converts the committed notebooks as-is (they carry no cell
outputs). It needs neither a built `uni_db` wheel nor a Jupyter kernel and
leaves the tracked `.ipynb` files unchanged. To render pages with executed
outputs, as the published site does, drop `--no-execute`. That mode runs
each Python notebook in a `python3` kernel, so `uni_db`, `uni_pydantic`,
`nbclient` and `ipykernel` must be importable, and it writes the outputs
back into the `.ipynb` files. Do not commit those output changes.

## Structure

```
website/
├── docs/                    # Documentation source files
│   ├── index.md            # Home page
│   ├── getting-started/    # Installation, quickstart, CLI
│   ├── concepts/           # Architecture, data model, etc.
│   ├── guides/             # Cypher, vector search, ingestion
│   ├── internals/          # Implementation details
│   ├── reference/          # API, config, troubleshooting
│   └── assets/             # Custom CSS and JS
├── scripts/                # Build and notebook generation scripts
├── mkdocs.yml              # Zensical configuration (backward-compatible format)
└── pyproject.toml          # Python dependencies
```

## Development

The site uses [Zensical](https://zensical.org/) (from the creators of Material for MkDocs).

### Local Preview

```bash
# Re-run after adding or changing a notebook
uv run --no-project --with nbconvert python scripts/convert_notebooks.py --no-execute

uv run --no-project --with zensical --with pymdown-extensions zensical serve
```

Visit http://localhost:8000

### Building

```bash
uv run --no-project --with nbconvert python scripts/convert_notebooks.py --no-execute
uv run --no-project --with zensical --with pymdown-extensions zensical build --strict
```

Output goes to `site/` directory (gitignored). A clean build reports
`No issues found`.

### Locy Notebook Generation

Locy example notebooks are generated and should not be edited manually.

```bash
# Regenerate Locy Python + Rust notebooks
uv run python website/scripts/generate_locy_notebooks.py

# Verify notebooks are in sync (used in PR checks)
uv run python website/scripts/generate_locy_notebooks.py --check

# Generate the flagship semiconductor Locy notebook
uv run python website/scripts/generate_semiconductor_flagship_notebook.py

# Execute + validate flagship notebook outputs
uv run --with ./bindings/uni-db python website/scripts/verify_semiconductor_flagship_notebook.py

# Prepare pharma flagship dataset bundle
uv run python website/scripts/prepare_pharma_batch_genealogy_data.py

# Generate flagship #2 pharma Locy notebook
uv run python website/scripts/generate_pharma_flagship_notebook.py

# Execute + validate flagship #2 notebook outputs
uv run --with ./bindings/uni-db python website/scripts/verify_pharma_flagship_notebook.py

# Prepare cyber flagship #3 dataset bundle
uv run python website/scripts/prepare_cyber_exposure_twin_data.py

# Generate flagship #3 cyber Locy notebook
uv run python website/scripts/generate_cyber_flagship_notebook.py

# Execute + validate flagship #3 notebook outputs
uv run --with ./bindings/uni-db python website/scripts/verify_cyber_flagship_notebook.py
```

## Deployment

The site is deployed to GitHub Pages via the `deploy-docs` GitHub Actions workflow. It runs automatically on releases and can be triggered manually via `workflow_dispatch`.
