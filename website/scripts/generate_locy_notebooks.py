#!/usr/bin/env python3
"""Generate and verify Locy example notebooks for website docs."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LocyUseCase:
    slug: str
    title: str
    summary: str
    python_schema_lines: list[str]
    rust_schema_lines: list[str]
    seed_statements: list[str]
    program_lines: list[str]
    expected_outcomes: list[str]


def _cell_id(notebook_key: str, index: int, cell_type: str) -> str:
    raw = f"{notebook_key}:{index}:{cell_type}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def _source_lines(lines: list[str]) -> list[str]:
    return [f"{line}\n" for line in lines]


def _md_cell(notebook_key: str, index: int, lines: list[str]) -> dict[str, Any]:
    return {
        "id": _cell_id(notebook_key, index, "markdown"),
        "cell_type": "markdown",
        "metadata": {},
        "source": _source_lines(lines),
    }


def _code_cell(notebook_key: str, index: int, lines: list[str]) -> dict[str, Any]:
    return {
        "id": _cell_id(notebook_key, index, "code"),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _source_lines(lines),
    }


def _python_metadata() -> dict[str, Any]:
    return {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "codemirror_mode": {"name": "ipython", "version": 3},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.11.0",
        },
    }


def _rust_metadata() -> dict[str, Any]:
    return {
        "kernelspec": {
            "display_name": "Rust",
            "language": "rust",
            "name": "rust",
        },
        "language_info": {
            "name": "rust",
            "pygments_lexer": "rust",
        },
    }


def _create_notebook(
    cells: list[dict[str, Any]], metadata: dict[str, Any]
) -> dict[str, Any]:
    return {
        "cells": cells,
        "metadata": metadata,
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _python_setup_lines() -> list[str]:
    return [
        "import os",
        "import shutil",
        "import tempfile",
        "from pprint import pprint",
        "",
        "import uni_db",
        "",
        'DB_DIR = tempfile.mkdtemp(prefix="uni_locy_")',
        'print("DB_DIR:", DB_DIR)',
        "",
        "db = uni_db.Uni.open(DB_DIR)",
        "session = db.session()",
    ]


def _python_seed_lines(statements: list[str]) -> list[str]:
    lines = ["tx = session.tx()"]
    lines.extend(f"tx.execute({json.dumps(stmt)})" for stmt in statements)
    lines.append("tx.commit()")
    lines.append("print('Seeded graph data')")
    return lines


def _python_schema_lines(schema_lines: list[str]) -> list[str]:
    return [
        "(",
        "    db.schema()",
        *schema_lines,
        "    .apply()",
        ")",
        "",
        "print('Schema created')",
    ]


def _rust_setup_lines() -> list[str]:
    return [
        "use uni_db::{DataType, Uni, Result};",
        "",
        "let db = Uni::in_memory().build().await?;",
    ]


def _rust_seed_lines(statements: list[str]) -> list[str]:
    lines = ["let session = db.session();"]
    lines.append("let tx = session.tx().await?;")
    lines.extend(f"tx.execute({json.dumps(stmt)}).await?;" for stmt in statements)
    lines.append("tx.commit().await?;")
    lines.append('println!("Seeded graph data");')
    return lines


def _rust_schema_lines(schema_lines: list[str]) -> list[str]:
    return [
        "db.schema()",
        *schema_lines,
        "    .apply()",
        "    .await?;",
        "",
        'println!("Schema created");',
    ]


def _python_program_lines(program_lines: list[str]) -> list[str]:
    return [
        "program = r'''",
        *program_lines,
        "'''",
        "print(program)",
    ]


def _rust_program_lines(program_lines: list[str]) -> list[str]:
    # Keep one notebook source entry per physical Rust line, like other cells.
    return ('let program = r#"' + "\n".join(program_lines) + '"#;').splitlines()


def _python_eval_lines() -> list[str]:
    return [
        "out = session.locy(program)",
        "",
        'print("Derived relations:", list(out.derived.keys()))',
        "stats = out.stats",
        'print("Iterations:", stats.total_iterations)',
        'print("Strata:", stats.strata_evaluated)',
        'print("Queries executed:", stats.queries_executed)',
    ]


def _python_results_lines() -> list[str]:
    return [
        'print("Derived relation snapshots:")',
        "for rel_name, rel_rows in out.derived.items():",
        '    print(f"\\\\n{rel_name}: {len(rel_rows)} row(s)")',
        "    pprint(rel_rows)",
        "",
        "if out.command_results:",
        '    print("\\\\nCommand results:")',
        "for i, cmd in enumerate(out.command_results, start=1):",
        '    print(f"\\\\nCommand #{i}:", cmd.command_type)',
        "    rows = getattr(cmd, 'rows', None)",
        "    if rows is not None:",
        "        pprint(rows)",
        "if not out.command_results:",
        '    print("\\\\nNo QUERY/EXPLAIN/ABDUCE command outputs in this program.")',
    ]


def _python_cleanup_lines() -> list[str]:
    return [
        "shutil.rmtree(DB_DIR, ignore_errors=True)",
        'print("Cleaned up", DB_DIR)',
    ]


def _rust_eval_lines() -> list[str]:
    return [
        "let session = db.session();",
        "let result = session.locy(program).await?;",
        'println!("Derived relations: {:?}", result.derived_rules().await?.keys().collect::<Vec<_>>());',
        'println!("Iterations: {}", result.stats().total_iterations);',
        'println!("Queries executed: {}", result.stats().queries_executed);',
        "for (name, rows) in result.derived_rules().await? {",
        '    println!("{}: {} row(s)", name, rows.len());',
        "}",
        "",
        "if let Some(rows) = result.rows() {",
        '    println!("Rows: {:?}", rows);',
        "}",
    ]


def _expected_markdown(case: LocyUseCase, step_number: int) -> list[str]:
    return [
        f"## {step_number}) What To Expect",
        "",
        "Use these checks to validate output after evaluation:",
        *[f"- {item}" for item in case.expected_outcomes],
    ]


def _python_notebook(case: LocyUseCase) -> dict[str, Any]:
    notebook_key = f"python:locy_{case.slug}"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                f"# Locy Use Case: {case.title}",
                "",
                case.summary,
                "",
                "This notebook uses **schema-first mode** (recommended): labels, edge types, and typed properties are defined before ingest.",
            ],
        )
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## How To Read This Notebook",
                "",
                "- Step 1 initializes an isolated local database.",
                "- Step 2 defines schema (the recommended production path).",
                "- Step 3 seeds a minimal graph for this use case.",
                "- Step 4 declares Locy rules and query statements.",
                "- Steps 5-6 evaluate and inspect command/query outputs.",
                "- Step 7 tells you what to look for in the results.",
            ],
        )
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 1) Setup",
                "",
                "Creates a temporary database directory so the example is reproducible and leaves no state behind.",
            ],
        )
    )
    cells.append(_code_cell(notebook_key, len(cells), _python_setup_lines()))

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 2) Define Schema (Recommended)",
                "",
                "Define labels, property types, and edge types before inserting data.",
            ],
        )
    )
    cells.append(
        _code_cell(
            notebook_key, len(cells), _python_schema_lines(case.python_schema_lines)
        )
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 3) Seed Graph Data",
                "",
                "Insert only the entities/relationships needed for this scenario so rule behavior stays easy to inspect.",
            ],
        )
    )
    cells.append(
        _code_cell(notebook_key, len(cells), _python_seed_lines(case.seed_statements))
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 4) Locy Program",
                "",
                "`CREATE RULE` defines derived relations. `QUERY ... WHERE ... RETURN ...` reads from those relations.",
            ],
        )
    )
    cells.append(
        _code_cell(notebook_key, len(cells), _python_program_lines(case.program_lines))
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 5) Evaluate Locy Program",
                "",
                "Run the program, then inspect materialization stats (`iterations`, `strata`, and executed queries).",
            ],
        )
    )
    cells.append(_code_cell(notebook_key, len(cells), _python_eval_lines()))

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 6) Inspect Command Results",
                "",
                "Each command result can contain `rows`; this is the easiest way to verify your rule outputs and query projections.",
            ],
        )
    )
    cells.append(_code_cell(notebook_key, len(cells), _python_results_lines()))

    cells.append(_md_cell(notebook_key, len(cells), _expected_markdown(case, 7)))

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 8) Cleanup",
                "",
                "Delete the temporary database directory created in setup.",
            ],
        )
    )
    cells.append(_code_cell(notebook_key, len(cells), _python_cleanup_lines()))

    return _create_notebook(cells, _python_metadata())


def _rust_notebook(case: LocyUseCase) -> dict[str, Any]:
    notebook_key = f"rust:locy_{case.slug}"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                f"# Locy Use Case: {case.title} (Rust)",
                "",
                case.summary,
                "",
                "This notebook uses **schema-first mode** and mirrors the Python flow using the Rust API (`uni_db`).",
            ],
        )
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## How To Read This Notebook",
                "",
                "- Define schema first, then load data.",
                "- Keep Locy rules declarative and focused.",
                "- Read output rows together with materialization stats.",
            ],
        )
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 1) Setup",
                "",
                "Initialize an in-memory database and import `DataType` for schema definitions.",
            ],
        )
    )
    cells.append(_code_cell(notebook_key, len(cells), _rust_setup_lines()))

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 2) Define Schema (Recommended)",
                "",
                "Define labels, typed properties, and edge types before inserting graph facts.",
            ],
        )
    )
    cells.append(
        _code_cell(notebook_key, len(cells), _rust_schema_lines(case.rust_schema_lines))
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 3) Seed Graph Data",
                "",
                "Insert the minimal graph needed for the scenario.",
            ],
        )
    )
    cells.append(
        _code_cell(notebook_key, len(cells), _rust_seed_lines(case.seed_statements))
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 4) Locy Program",
                "",
                "Rules derive relations, then `QUERY ... WHERE ... RETURN ...` projects the final answer.",
            ],
        )
    )
    cells.append(
        _code_cell(notebook_key, len(cells), _rust_program_lines(case.program_lines))
    )

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## 5) Evaluate",
                "",
                "Evaluate the Locy program and inspect stats/rows.",
            ],
        )
    )
    cells.append(_code_cell(notebook_key, len(cells), _rust_eval_lines()))

    cells.append(_md_cell(notebook_key, len(cells), _expected_markdown(case, 6)))

    cells.append(
        _md_cell(
            notebook_key,
            len(cells),
            [
                "## Notes",
                "",
                "- Rust notebooks are included for API parity and learning.",
                "- In this docs build, Rust notebooks are rendered without execution.",
            ],
        )
    )

    return _create_notebook(cells, _rust_metadata())


def _cases() -> list[LocyUseCase]:
    return [
        LocyUseCase(
            slug="compliance_remediation",
            title="Compliance Remediation",
            summary="Compute exposed + vulnerable services and emit prioritized remediation actions.",
            python_schema_lines=[
                '    .label("Internet")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("Service")',
                '        .property("name", "string")',
                '        .property("cve_score", "float64")',
                "    .done()",
                '    .edge_type("EXPOSES", ["Internet"], ["Service"])',
                "    .done()",
                '    .edge_type("DEPENDS_ON", ["Service"], ["Service"])',
                "    .done()",
            ],
            rust_schema_lines=[
                '    .label("Internet")',
                '        .property("name", DataType::String)',
                '    .label("Service")',
                '        .property("name", DataType::String)',
                '        .property("cve_score", DataType::Float64)',
                '    .edge_type("EXPOSES", &["Internet"], &["Service"])',
                '    .edge_type("DEPENDS_ON", &["Service"], &["Service"])',
            ],
            seed_statements=[
                "CREATE (:Internet {name: 'public'})",
                "CREATE (:Service {name: 'api', cve_score: 9.1})",
                "CREATE (:Service {name: 'worker', cve_score: 4.0})",
                "CREATE (:Service {name: 'db', cve_score: 8.4})",
                "MATCH (i:Internet {name:'public'}), (s:Service {name:'api'}) CREATE (i)-[:EXPOSES]->(s)",
                "MATCH (a:Service {name:'api'}), (w:Service {name:'worker'}) CREATE (a)-[:DEPENDS_ON]->(w)",
                "MATCH (w:Service {name:'worker'}), (d:Service {name:'db'}) CREATE (w)-[:DEPENDS_ON]->(d)",
            ],
            program_lines=[
                "CREATE RULE depends_on AS",
                "MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)",
                "YIELD KEY a, KEY b",
                "",
                "CREATE RULE depends_on AS",
                "MATCH (a:Service)-[:DEPENDS_ON]->(mid:Service)",
                "WHERE mid IS depends_on TO b",
                "YIELD KEY a, KEY b",
                "",
                "CREATE RULE exposed AS",
                "MATCH (i:Internet)-[:EXPOSES]->(s:Service)",
                "YIELD KEY s",
                "",
                "CREATE RULE exposed AS",
                "MATCH (i:Internet)-[:EXPOSES]->(entry:Service)",
                "WHERE entry IS depends_on TO s",
                "YIELD KEY s",
                "",
                "CREATE RULE vulnerable AS",
                "MATCH (s:Service)",
                "WHERE s.cve_score >= 7",
                "YIELD KEY s",
                "",
                "CREATE RULE non_compliant AS",
                "MATCH (s:Service)",
                "WHERE s IS vulnerable, s IS exposed",
                "YIELD KEY s, 'patch-now' AS action",
                "",
                "QUERY non_compliant WHERE s.name = s.name RETURN s.name AS service, action",
            ],
            expected_outcomes=[
                "`non_compliant` should include `api` and `db` (reachable from internet and CVE >= 7).",
                "`worker` should not appear in remediation rows because its CVE score is below threshold.",
                "The `non_compliant` QUERY returns `action = 'patch-now'`; this literal is computed in the query projection, so it may display as `None` in the raw derived-relation snapshot (which materializes KEY columns).",
                "`Queries executed` counts the Cypher statements a program runs and the pattern queries `EXPLAIN RULE` issues when it rebuilds a tree from the facts. A `QUERY` reads facts the fixpoint already derived and is not counted, so this program reports 0.",
            ],
        ),
        LocyUseCase(
            slug="rbac_priority",
            title="RBAC with Priority Rules",
            summary="Resolve deny-vs-allow authorization conflicts with prioritized Locy clauses.",
            python_schema_lines=[
                '    .label("User")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("Resource")',
                '        .property("name", "string")',
                "    .done()",
                '    .edge_type("ALLOWED", ["User"], ["Resource"])',
                "    .done()",
                '    .edge_type("DENIED", ["User"], ["Resource"])',
                "    .done()",
            ],
            rust_schema_lines=[
                '    .label("User")',
                '        .property("name", DataType::String)',
                '    .label("Resource")',
                '        .property("name", DataType::String)',
                '    .edge_type("ALLOWED", &["User"], &["Resource"])',
                '    .edge_type("DENIED", &["User"], &["Resource"])',
            ],
            seed_statements=[
                "CREATE (:User {name: 'alice'})",
                "CREATE (:User {name: 'bob'})",
                "CREATE (:Resource {name: 'prod-db'})",
                "MATCH (u:User {name:'alice'}), (r:Resource {name:'prod-db'}) CREATE (u)-[:ALLOWED]->(r)",
                "MATCH (u:User {name:'bob'}), (r:Resource {name:'prod-db'}) CREATE (u)-[:ALLOWED]->(r)",
                "MATCH (u:User {name:'bob'}), (r:Resource {name:'prod-db'}) CREATE (u)-[:DENIED]->(r)",
            ],
            program_lines=[
                "CREATE RULE access PRIORITY 1 AS",
                "MATCH (u:User)-[:ALLOWED]->(r:Resource)",
                "YIELD KEY u, KEY r, 1 AS decision_code",
                "",
                "CREATE RULE access PRIORITY 2 AS",
                "MATCH (u:User)-[:DENIED]->(r:Resource)",
                "YIELD KEY u, KEY r, 2 AS decision_code",
            ],
            expected_outcomes=[
                "In `derived['access']`, `alice` should have `decision_code = 1` (ALLOW path).",
                "In `derived['access']`, `bob` should have `decision_code = 2` (DENY override).",
                "Exactly one derived row per `(user, resource)` should remain after priority filtering.",
            ],
        ),
        LocyUseCase(
            slug="infrastructure_blast_radius",
            title="Infrastructure Blast Radius",
            summary="Compute transitive downstream impact from a failing upstream service.",
            python_schema_lines=[
                '    .label("Service")',
                '        .property("name", "string")',
                "    .done()",
                '    .edge_type("CALLS", ["Service"], ["Service"])',
                "    .done()",
            ],
            rust_schema_lines=[
                '    .label("Service")',
                '        .property("name", DataType::String)',
                '    .edge_type("CALLS", &["Service"], &["Service"])',
            ],
            seed_statements=[
                "CREATE (:Service {name: 'api'})",
                "CREATE (:Service {name: 'gateway'})",
                "CREATE (:Service {name: 'worker'})",
                "CREATE (:Service {name: 'db'})",
                "CREATE (:Service {name: 'cache'})",
                "MATCH (a:Service {name:'api'}), (g:Service {name:'gateway'}) CREATE (a)-[:CALLS]->(g)",
                "MATCH (g:Service {name:'gateway'}), (w:Service {name:'worker'}) CREATE (g)-[:CALLS]->(w)",
                "MATCH (w:Service {name:'worker'}), (d:Service {name:'db'}) CREATE (w)-[:CALLS]->(d)",
                "MATCH (w:Service {name:'worker'}), (c:Service {name:'cache'}) CREATE (w)-[:CALLS]->(c)",
            ],
            program_lines=[
                "CREATE RULE impacts AS",
                "MATCH (a:Service)-[:CALLS]->(b:Service)",
                "YIELD KEY a, KEY b",
                "",
                "CREATE RULE impacts AS",
                "MATCH (a:Service)-[:CALLS]->(mid:Service)",
                "WHERE mid IS impacts TO b",
                "YIELD KEY a, KEY b",
                "",
                "QUERY impacts WHERE a.name = 'api' RETURN b.name AS impacted_service",
            ],
            expected_outcomes=[
                "For `api`, impacted services should include `gateway`, `worker`, `db`, and `cache`.",
                "Rows should represent transitive reachability, not only direct neighbors.",
                "This pattern is useful for outage simulation and dependency triage.",
            ],
        ),
        LocyUseCase(
            slug="supply_chain_provenance",
            title="Supply Chain Provenance",
            summary="Trace multi-hop upstream supplier lineage for a finished component.",
            python_schema_lines=[
                '    .label("Part")',
                '        .property("sku", "string")',
                '        .property("kind", "string")',
                "    .done()",
                '    .edge_type("SOURCED_FROM", ["Part"], ["Part"])',
                "    .done()",
            ],
            rust_schema_lines=[
                '    .label("Part")',
                '        .property("sku", DataType::String)',
                '        .property("kind", DataType::String)',
                '    .edge_type("SOURCED_FROM", &["Part"], &["Part"])',
            ],
            seed_statements=[
                "CREATE (:Part {sku: 'C1', kind: 'finished'})",
                "CREATE (:Part {sku: 'B1', kind: 'subassembly'})",
                "CREATE (:Part {sku: 'B2', kind: 'subassembly'})",
                "CREATE (:Part {sku: 'R1', kind: 'raw'})",
                "CREATE (:Part {sku: 'R2', kind: 'raw'})",
                "MATCH (c:Part {sku:'C1'}), (b1:Part {sku:'B1'}) CREATE (c)-[:SOURCED_FROM]->(b1)",
                "MATCH (c:Part {sku:'C1'}), (b2:Part {sku:'B2'}) CREATE (c)-[:SOURCED_FROM]->(b2)",
                "MATCH (b1:Part {sku:'B1'}), (r1:Part {sku:'R1'}) CREATE (b1)-[:SOURCED_FROM]->(r1)",
                "MATCH (b2:Part {sku:'B2'}), (r2:Part {sku:'R2'}) CREATE (b2)-[:SOURCED_FROM]->(r2)",
            ],
            program_lines=[
                "CREATE RULE upstream AS",
                "MATCH (a:Part)-[:SOURCED_FROM]->(b:Part)",
                "YIELD KEY a, KEY b",
                "",
                "CREATE RULE upstream AS",
                "MATCH (a:Part)-[:SOURCED_FROM]->(mid:Part)",
                "WHERE mid IS upstream TO b",
                "YIELD KEY a, KEY b",
                "",
                "QUERY upstream WHERE a.sku = 'C1' RETURN b.sku AS supplier_sku, b.kind AS supplier_kind",
            ],
            expected_outcomes=[
                "For `C1`, output should include both subassemblies (`B1`, `B2`) and raw parts (`R1`, `R2`).",
                "`supplier_kind` helps separate immediate suppliers vs deeper upstream tiers.",
                "This same pattern scales to provenance and recall workflows.",
            ],
        ),
        LocyUseCase(
            slug="fraud_risk_propagation",
            title="Fraud Risk Propagation",
            summary="Propagate account risk backward over transfer edges and isolate clean accounts.",
            python_schema_lines=[
                '    .label("Account")',
                '        .property("id", "string")',
                '        .property("flagged", "bool")',
                "    .done()",
                '    .edge_type("TRANSFER", ["Account"], ["Account"])',
                "    .done()",
            ],
            rust_schema_lines=[
                '    .label("Account")',
                '        .property("id", DataType::String)',
                '        .property("flagged", DataType::Bool)',
                '    .edge_type("TRANSFER", &["Account"], &["Account"])',
            ],
            seed_statements=[
                "CREATE (:Account {id: 'A1', flagged: true})",
                "CREATE (:Account {id: 'A2', flagged: false})",
                "CREATE (:Account {id: 'A3', flagged: false})",
                "CREATE (:Account {id: 'A4', flagged: false})",
                "MATCH (a2:Account {id:'A2'}), (a1:Account {id:'A1'}) CREATE (a2)-[:TRANSFER]->(a1)",
                "MATCH (a4:Account {id:'A4'}), (a2:Account {id:'A2'}) CREATE (a4)-[:TRANSFER]->(a2)",
                "MATCH (a1:Account {id:'A1'}), (a3:Account {id:'A3'}) CREATE (a1)-[:TRANSFER]->(a3)",
            ],
            program_lines=[
                "CREATE RULE risky_seed AS",
                "MATCH (a:Account)",
                "WHERE a.flagged = true",
                "YIELD KEY a",
                "",
                "CREATE RULE risky AS",
                "MATCH (a:Account)",
                "WHERE a IS risky_seed",
                "YIELD KEY a",
                "",
                "CREATE RULE risky AS",
                "MATCH (a:Account)-[:TRANSFER]->(b:Account)",
                "WHERE b IS risky",
                "YIELD KEY a",
                "",
                "CREATE RULE clean AS",
                "MATCH (a:Account)",
                "WHERE a IS NOT risky",
                "YIELD KEY a",
                "",
                "QUERY risky WHERE a.id = a.id RETURN a.id AS risky_account",
                "QUERY clean WHERE a.id = a.id RETURN a.id AS clean_account",
            ],
            expected_outcomes=[
                "`A1` is risky by seed. `A2` is risky because it transfers to `A1`; `A4` is risky *transitively* because it transfers to `A2`. This is **backward propagation** — risk flows from a flagged account to the accounts that transfer into it (directly or through a chain).",
                "`A3` should remain in `clean`: it only *receives* from `A1` and never transfers to a risky account.",
                "Two query result blocks should appear: one for `risky` (A1, A2, A4), one for `clean` (A3).",
            ],
        ),
        LocyUseCase(
            slug="probabilistic_risk_scoring",
            title="Probabilistic Risk Scoring",
            summary="Evaluate vendor reliability by combining independent quality signals with MNOR (noisy-OR failure) and MPROD (joint reliability).",
            python_schema_lines=[
                '    .label("Vendor")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("Component")',
                '        .property("name", "string")',
                "    .done()",
                '    .label("QualitySignal")',
                '        .property("name", "string")',
                '        .property("pass_rate", "float64")',
                "    .done()",
                '    .edge_type("SUPPLIES", ["Vendor"], ["Component"])',
                "    .done()",
                '    .edge_type("HAS_SIGNAL", ["Component"], ["QualitySignal"])',
                "    .done()",
            ],
            rust_schema_lines=[
                '    .label("Vendor")',
                '        .property("name", DataType::String)',
                '    .label("Component")',
                '        .property("name", DataType::String)',
                '    .label("QualitySignal")',
                '        .property("name", DataType::String)',
                '        .property("pass_rate", DataType::Float64)',
                '    .edge_type("SUPPLIES", &["Vendor"], &["Component"])',
                '    .edge_type("HAS_SIGNAL", &["Component"], &["QualitySignal"])',
            ],
            seed_statements=[
                # Vendors
                "CREATE (:Vendor {name: 'ReliaCorp'})",
                "CREATE (:Vendor {name: 'QuickParts'})",
                "CREATE (:Vendor {name: 'BudgetSupply'})",
                # Components
                "CREATE (:Component {name: 'Sensor'})",
                "CREATE (:Component {name: 'Motor'})",
                "CREATE (:Component {name: 'Controller'})",
                "CREATE (:Component {name: 'Battery'})",
                # Quality signals with pass_rate probabilities
                "CREATE (:QualitySignal {name: 'Thermal Test', pass_rate: 0.95})",
                "CREATE (:QualitySignal {name: 'Vibration Test', pass_rate: 0.90})",
                "CREATE (:QualitySignal {name: 'Voltage Tolerance', pass_rate: 0.85})",
                "CREATE (:QualitySignal {name: 'Humidity Test', pass_rate: 0.92})",
                "CREATE (:QualitySignal {name: 'Load Test', pass_rate: 0.88})",
                "CREATE (:QualitySignal {name: 'EMC Test', pass_rate: 0.75})",
                "CREATE (:QualitySignal {name: 'Cycle Life', pass_rate: 0.80})",
                "CREATE (:QualitySignal {name: 'Drop Test', pass_rate: 0.70})",
                # Vendor -> Component edges
                "MATCH (v:Vendor {name:'ReliaCorp'}), (c:Component {name:'Sensor'}) CREATE (v)-[:SUPPLIES]->(c)",
                "MATCH (v:Vendor {name:'ReliaCorp'}), (c:Component {name:'Motor'}) CREATE (v)-[:SUPPLIES]->(c)",
                "MATCH (v:Vendor {name:'QuickParts'}), (c:Component {name:'Motor'}) CREATE (v)-[:SUPPLIES]->(c)",
                "MATCH (v:Vendor {name:'QuickParts'}), (c:Component {name:'Controller'}) CREATE (v)-[:SUPPLIES]->(c)",
                "MATCH (v:Vendor {name:'BudgetSupply'}), (c:Component {name:'Controller'}) CREATE (v)-[:SUPPLIES]->(c)",
                "MATCH (v:Vendor {name:'BudgetSupply'}), (c:Component {name:'Battery'}) CREATE (v)-[:SUPPLIES]->(c)",
                # Component -> QualitySignal edges
                "MATCH (c:Component {name:'Sensor'}), (s:QualitySignal {name:'Thermal Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)",
                "MATCH (c:Component {name:'Sensor'}), (s:QualitySignal {name:'Vibration Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)",
                "MATCH (c:Component {name:'Motor'}), (s:QualitySignal {name:'Voltage Tolerance'}) CREATE (c)-[:HAS_SIGNAL]->(s)",
                "MATCH (c:Component {name:'Motor'}), (s:QualitySignal {name:'Humidity Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)",
                "MATCH (c:Component {name:'Controller'}), (s:QualitySignal {name:'Load Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)",
                "MATCH (c:Component {name:'Controller'}), (s:QualitySignal {name:'EMC Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)",
                "MATCH (c:Component {name:'Battery'}), (s:QualitySignal {name:'Cycle Life'}) CREATE (c)-[:HAS_SIGNAL]->(s)",
                "MATCH (c:Component {name:'Battery'}), (s:QualitySignal {name:'Drop Test'}) CREATE (c)-[:HAS_SIGNAL]->(s)",
            ],
            program_lines=[
                "CREATE RULE component_failure_risk AS",
                "MATCH (c:Component)-[:HAS_SIGNAL]->(s:QualitySignal)",
                "FOLD risk = MNOR(1.0 - s.pass_rate)",
                "YIELD KEY c, risk",
                "",
                "CREATE RULE vendor_reliability AS",
                "MATCH (v:Vendor)-[:SUPPLIES]->(c:Component)",
                "WHERE c IS component_failure_risk",
                "FOLD reliability = MPROD(1.0 - risk)",
                "YIELD KEY v, reliability",
                "",
                "QUERY component_failure_risk RETURN c.name AS component, risk",
                "QUERY vendor_reliability RETURN v.name AS vendor, reliability",
            ],
            expected_outcomes=[
                "Component risk ordering: Battery > Controller > Motor > Sensor (lower pass rates → higher risk).",
                "Vendor reliability ordering: ReliaCorp > QuickParts > BudgetSupply.",
                "MNOR values stay in [0, 1] — noisy-OR never exceeds 1.0 even with many signals.",
                "MPROD values decrease with more components — each additional component can only reduce joint reliability.",
                "Two query result blocks should appear: one for `component_failure_risk`, one for `vendor_reliability`.",
            ],
        ),
    ]


def _render_json(obj: dict[str, Any]) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False) + "\n"


def _all_targets() -> dict[Path, str]:
    website_dir = Path(__file__).resolve().parents[1]
    python_dir = website_dir / "docs" / "examples" / "python"
    rust_dir = website_dir / "docs" / "examples" / "rust"

    targets: dict[Path, str] = {}
    for case in _cases():
        py_path = python_dir / f"locy_{case.slug}.ipynb"
        rs_path = rust_dir / f"locy_{case.slug}.ipynb"
        targets[py_path] = _render_json(_python_notebook(case))
        targets[rs_path] = _render_json(_rust_notebook(case))
    return targets


def _write_targets(targets: dict[Path, str]) -> int:
    for path, content in sorted(targets.items()):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        print(f"generated {path}")
    return 0


def _check_targets(targets: dict[Path, str]) -> int:
    mismatches = 0
    for path, expected in sorted(targets.items()):
        actual = path.read_text(encoding="utf-8") if path.exists() else ""
        if actual == expected:
            continue

        mismatches += 1
        print(f"drift detected: {path}")
        diff = list(
            difflib.unified_diff(
                actual.splitlines(),
                expected.splitlines(),
                fromfile=str(path),
                tofile=f"{path} (generated)",
                lineterm="",
                n=2,
            )
        )
        for line in diff[:80]:
            print(line)
        if len(diff) > 80:
            print("... diff truncated ...")

    if mismatches:
        print(f"\n{mismatches} file(s) are out of date.")
        print("Run: python3 website/scripts/generate_locy_notebooks.py")
        return 1

    print("Locy notebooks are up to date.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check for drift without writing files (non-zero exit on mismatch).",
    )
    args = parser.parse_args(argv)

    targets = _all_targets()
    if args.check:
        return _check_targets(targets)
    return _write_targets(targets)


if __name__ == "__main__":
    sys.exit(main())
