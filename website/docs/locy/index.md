# Locy

Locy is Uni's logic programming layer: **Logic + Cypher**. It extends OpenCypher with recursive rules, hypothetical reasoning, graph derivation, and abductive remediation workflows.

## Who This Is For

- Application engineers who need graph reasoning beyond one-shot queries.
- Data and policy teams modeling compliance, access control, and propagation rules.
- Advanced users who want compiler/runtime internals and TCK-level semantics.

## What You Get

- Declarative rules with recursion (`CREATE RULE`).
- Queries over the derived facts (`QUERY`).
- Hypothetical analysis (`ASSUME ... THEN`).
- Remediation search (`ABDUCE`).
- Derivation/proof explainability (`EXPLAIN RULE`).
- Graph materialization (`DERIVE`).
- Probabilistic composition (`MNOR`, `MPROD`, `PROB`).
- Inline neural predicates (`CREATE MODEL`, `CALIBRATE`, `VALIDATE`).

## Start Here

1. Read [Foundations](foundations.md).
2. Run the [Quickstart](quickstart.md).
3. Learn syntax in [Language Guide](language-guide.md).
4. Move to [Advanced Features](advanced/along-fold-bestby.md).
5. For probabilistic and neural composition, see [Probabilistic Logic](advanced/probabilistic-logic.md) and [Neural Predicates](advanced/neural-predicates.md).

## Locy vs Plain Cypher

Use plain Cypher when you need direct reads/writes on existing graph state. Use Locy when you need:

- Recursive closure and fixed-point reasoning.
- Explainable inferred facts.
- What-if analysis with rollback boundaries.
- Suggested single edits (one edge or property change, each validated) that satisfy or prevent outcomes.

## API Entry Points

- **Rust:** `session.locy(program).await?` and the `LocyBuilder` fluent API via `session.locy_with(program).param(...).timeout(...).with_config(...).run().await?`.
- **Python (sync):** `session.locy(program)` and the fluent API via `session.locy_with(program).param(...).with_config(...).run()`.
- **Python (async):** `await session.locy(program)` and `await session.locy_with(program).run()`.

Sessions are created from a `Uni` instance: `session = db.session()`.

See [Rust API Integration](api/rust.md) and [Python API Integration](api/python.md).
