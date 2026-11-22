# uni-db — unreleased

Changes since [5.3.0](RELEASE_NOTES_5.3.0.md).

Add new changes here.

## Fixes

- **OGM:** a `Vector[N] | None` field is a nullable vector column, so saving a node without a vector no longer fails with "cannot be null".
- **OGM:** `SparseVector[N]` fields are saved as `uni_db.SparseVector`; a `TypeMappingError` is raised if the bindings lack that class.
- **OGM:** `tx.create_edge` accepts nodes added to the same transaction (sync and async). A failed commit clears the vid, session and identity-map entry of the nodes it had created.
- **CLI:** `uni import <name>` runs the importer the name selects; an unknown name fails and lists the available importers (`semantic-scholar`).
