# Uni Documentation

Documentation site for Uni, the embedded multi-model graph database.

## Setup

```bash
# Install dependencies
poetry install

# Serve locally with hot reload
poetry run mkdocs serve

# Build static site
poetry run mkdocs build
```

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
│   ├── stylesheets/        # Custom CSS
│   └── javascripts/        # Custom JS
├── mkdocs.yml              # MkDocs configuration
└── pyproject.toml          # Python dependencies
```

## Development

The site uses [MkDocs](https://www.mkdocs.org/) with the [Material](https://squidfunk.github.io/mkdocs-material/) theme.

### Local Preview

```bash
poetry run mkdocs serve
```

Visit http://localhost:8000

### Building

```bash
poetry run mkdocs build
```

Output goes to `site/` directory.

## Deployment

The site can be deployed to GitHub Pages:

```bash
poetry run mkdocs gh-deploy
```
