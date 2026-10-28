#!/usr/bin/env python3
"""Generate the flagship Locy semiconductor notebook."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


NOTEBOOK_PATH = Path(
    "website/docs/examples/python/locy_semiconductor_yield_excursion.ipynb"
)


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


def _metadata() -> dict[str, Any]:
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


def _create_notebook(cells: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "cells": cells,
        "metadata": _metadata(),
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _render_json(obj: dict[str, Any]) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False) + "\n"


def _build_notebook() -> dict[str, Any]:
    key = "python:locy_semiconductor_yield_excursion"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "# Locy Flagship: Semiconductor Yield Excursion Triage",
                "",
                "This notebook uses a **real manufacturing dataset** (SECOM, UCI) and walks end-to-end through:",
                "",
                "- `DERIVE`: materialize risk links into graph edges.",
                "- `ASSUME`: run temporary containment scenarios.",
                "- `ABDUCE`: propose minimal changes that alter outcomes.",
                "- `EXPLAIN RULE`: inspect proof paths behind a conclusion.",
                "",
                "It is schema-first (recommended) and designed for first-time Locy readers.",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## How To Read This Notebook",
                "",
                "- Each code cell is preceded by intent, expected output shape, and reading tips.",
                "- We use a curated slice from SECOM for quick execution in docs while preserving real data behavior.",
                "- Commands are grouped so you can reason from facts -> inference -> counterfactual -> explanation.",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 1) Setup and Data Discovery",
                "",
                "What this does:",
                "Initialize helper utilities, locate prepared data files, and create an isolated temporary database.",
                "",
                "What to expect:",
                "Printed `DATA_DIR` and `DB_DIR` paths.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "from pathlib import Path",
                "from pprint import pprint",
                "import csv",
                "import shutil",
                "import tempfile",
                "import os",
                "",
                "import uni_db",
                "",
                "def _read_csv(path: Path) -> list[dict[str, str]]:",
                "    with path.open('r', encoding='utf-8', newline='') as f:",
                "        return list(csv.DictReader(f))",
                "",
                "def _esc(value: str) -> str:",
                "    return str(value).replace('\\\\', '\\\\\\\\').replace(\"'\", \"\\\\'\")",
                "",
                "_default_candidates = [",
                "    Path('docs/examples/data/locy_semiconductor_yield_excursion'),",
                "    Path('website/docs/examples/data/locy_semiconductor_yield_excursion'),",
                "    Path('examples/data/locy_semiconductor_yield_excursion'),",
                "    Path('../data/locy_semiconductor_yield_excursion'),",
                "]",
                "if 'LOCY_DATA_DIR' in os.environ:",
                "    DATA_DIR = Path(os.environ['LOCY_DATA_DIR']).resolve()",
                "else:",
                "    DATA_DIR = next(",
                "        (p.resolve() for p in _default_candidates if (p / 'secom_lots.csv').exists()),",
                "        _default_candidates[0].resolve(),",
                "    )",
                "if not (DATA_DIR / 'secom_lots.csv').exists():",
                "    raise FileNotFoundError(",
                "        'Expected dataset under docs/examples/data/locy_semiconductor_yield_excursion. '",
                "        'Run from website/ (or repo root) or set LOCY_DATA_DIR to the dataset path.'",
                "    )",
                "DB_DIR = tempfile.mkdtemp(prefix='uni_locy_semiconductor_')",
                "db = uni_db.Uni.open(DB_DIR)",
                "session = db.session()",
                "",
                "print('DATA_DIR:', DATA_DIR)",
                "print('DB_DIR:', DB_DIR)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 2) Load Real Data and Build a Focus Slice",
                "",
                "What this does:",
                "Loads SECOM-derived CSVs and keeps a focused cohort (fail-heavy + pass references) for fast but meaningful execution.",
                "",
                "What to expect:",
                "Counts for lots, selected features, tools/modules, and excursion events.",
                "",
                "How to read it:",
                "The focused slice remains grounded in real measurements while keeping notebook runtime practical.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "lots = _read_csv(DATA_DIR / 'secom_lots.csv')",
                "features = _read_csv(DATA_DIR / 'secom_feature_catalog.csv')",
                "excursions = _read_csv(DATA_DIR / 'secom_excursions.csv')",
                "notebook_cases = _read_csv(DATA_DIR / 'secom_notebook_cases.csv')",
                "",
                "selected_features = {r['feature_id']: r for r in features if r['selected'].lower() == 'true'}",
                "focus_fail_ids = [r['lot_id'] for r in notebook_cases[:24]]",
                "pass_reference_ids = [r['lot_id'] for r in lots if r['yield_outcome'] == 'PASS'][:72]",
                "",
                "focus_ids = set(focus_fail_ids + pass_reference_ids)",
                "focus_lots = [r for r in lots if r['lot_id'] in focus_ids]",
                "focus_excursions = [",
                "    r for r in excursions",
                "    if r['lot_id'] in focus_ids and r['feature_id'] in selected_features",
                "]",
                "",
                "active_feature_ids = sorted({r['feature_id'] for r in focus_excursions})",
                "feature_rows = [selected_features[fid] for fid in active_feature_ids]",
                "",
                "tools = {}",
                "modules = set()",
                "for row in feature_rows:",
                "    tools[row['tool_id']] = row['module']",
                "    modules.add(row['module'])",
                "",
                "print('focus lots:', len(focus_lots))",
                "print('focus fail lots:', sum(1 for r in focus_lots if r['yield_outcome'] == 'FAIL'))",
                "print('selected active features:', len(feature_rows))",
                "print('tools:', len(tools), 'modules:', len(modules))",
                "print('focus excursion rows:', len(focus_excursions))",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 3) Define Schema (Recommended)",
                "",
                "What this does:",
                "Creates explicit labels, typed properties, and edge types before ingest.",
                "",
                "What to expect:",
                "A single `Schema created` confirmation.",
                "",
                "How to read it:",
                "Schema mode keeps demos and production behavior aligned and prevents implicit-shape drift.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "(",
                "    db.schema()",
                "    .label('Lot')",
                "        .property('lot_id', 'string')",
                "        .property('yield_outcome', 'string')",
                "        .property('test_timestamp', 'string')",
                "        .property('cohort', 'string')",
                "    .done()",
                "    .label('Feature')",
                "        .property('feature_id', 'string')",
                "        .property('module', 'string')",
                "        .property('tool_id', 'string')",
                "        .property('effect_size', 'float64')",
                "        .property('selected', 'bool')",
                "    .done()",
                "    .label('Tool')",
                "        .property('tool_id', 'string')",
                "        .property('module', 'string')",
                "    .done()",
                "    .label('Module')",
                "        .property('name', 'string')",
                "    .done()",
                "    .edge_type('OBSERVED_EXCURSION', ['Lot'], ['Feature'])",
                "    .done()",
                "    .edge_type('MEASURED_ON', ['Feature'], ['Tool'])",
                "    .done()",
                "    .edge_type('PART_OF', ['Tool'], ['Module'])",
                "    .done()",
                "    .edge_type('IMPACTS_TOOL', ['Lot'], ['Tool'])",
                "    .done()",
                "    .edge_type('CONTAINED_BY', ['Lot'], ['Tool'])",
                "    .done()",
                "    .apply()",
                ")",
                "",
                "print('Schema created')",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 4) Ingest the Manufacturing Graph",
                "",
                "What this does:",
                "Inserts module/tool/feature/lot facts and excursion edges for the focused real-data slice.",
                "",
                "What to expect:",
                "Graph counts for nodes and excursion edges.",
                "",
                "How to read it:",
                "Each `Lot -> Feature -> Tool -> Module` chain is the evidence path Locy will reason over.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "tx = session.tx()",
                "",
                "for module in sorted(modules):",
                "    tx.execute(f\"CREATE (:Module {{name: '{_esc(module)}'}})\")",
                "",
                "for tool_id, module in sorted(tools.items()):",
                "    tx.execute(",
                "        f\"CREATE (:Tool {{tool_id: '{_esc(tool_id)}', module: '{_esc(module)}'}})\"",
                "    )",
                "    tx.execute(",
                "        f\"MATCH (t:Tool {{tool_id: '{_esc(tool_id)}'}}), (m:Module {{name: '{_esc(module)}'}}) \"",
                '        "CREATE (t)-[:PART_OF]->(m)"',
                "    )",
                "",
                "for row in feature_rows:",
                "    selected_literal = 'true' if row['selected'].lower() == 'true' else 'false'",
                "    effect_size = float(row['effect_size']) if row['effect_size'] else 0.0",
                "    tx.execute(",
                "        f\"CREATE (:Feature {{feature_id: '{_esc(row['feature_id'])}', module: '{_esc(row['module'])}', \"",
                "        f\"tool_id: '{_esc(row['tool_id'])}', effect_size: {effect_size}, selected: {selected_literal}}})\"",
                "    )",
                "    tx.execute(",
                "        f\"MATCH (f:Feature {{feature_id: '{_esc(row['feature_id'])}'}}), (t:Tool {{tool_id: '{_esc(row['tool_id'])}'}}) \"",
                '        "CREATE (f)-[:MEASURED_ON]->(t)"',
                "    )",
                "",
                "for row in focus_lots:",
                "    cohort = 'fail_focus' if row['yield_outcome'] == 'FAIL' else 'pass_reference'",
                "    tx.execute(",
                "        f\"CREATE (:Lot {{lot_id: '{_esc(row['lot_id'])}', yield_outcome: '{_esc(row['yield_outcome'])}', \"",
                "        f\"test_timestamp: '{_esc(row['test_timestamp'])}', cohort: '{cohort}'}})\"",
                "    )",
                "",
                "for row in focus_excursions:",
                "    tx.execute(",
                "        f\"MATCH (l:Lot {{lot_id: '{_esc(row['lot_id'])}'}}), (f:Feature {{feature_id: '{_esc(row['feature_id'])}'}}) \"",
                '        "CREATE (l)-[:OBSERVED_EXCURSION]->(f)"',
                "    )",
                "",
                "tx.commit()",
                "",
                'counts = session.query("""',
                "MATCH (l:Lot)",
                "WITH count(l) AS lots",
                "MATCH (f:Feature)",
                "WITH lots, count(f) AS features",
                "MATCH ()-[e:OBSERVED_EXCURSION]->()",
                "RETURN lots, features, count(e) AS excursion_edges",
                '""")',
                "print('Graph counts:')",
                "pprint(counts[0])",
                "",
                'outcome_counts = session.query("""',
                "MATCH (l:Lot)",
                "RETURN l.yield_outcome AS outcome, count(*) AS lots",
                "ORDER BY lots DESC",
                '""")',
                "print('\\nLot outcomes:')",
                "pprint(outcome_counts)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 5) Baseline Inference + `DERIVE` Materialization",
                "",
                "What this does:",
                "Builds fail-lot relations, projects fail excursions to tools, and materializes `:IMPACTS_TOOL` edges via `DERIVE`.",
                "",
                "What to expect:",
                "- A `query` result listing `(lot_id, tool_id, module)` evidence rows.",
                "- A `derive` result with affected mutation count.",
                "- A `cypher` ranking of hotspot tools.",
                "",
                "How to read Locy rules:",
                "- `CREATE RULE ... YIELD` creates logical relations.",
                "- `CREATE RULE ... DERIVE` defines graph mutations that `DERIVE <rule>` executes.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "program_baseline = r'''",
                "CREATE RULE fail_lot AS",
                "MATCH (l:Lot)",
                "WHERE l.yield_outcome = 'FAIL'",
                "YIELD KEY l",
                "",
                "CREATE RULE fail_tool_excursion AS",
                "MATCH (l:Lot)-[:OBSERVED_EXCURSION]->(f:Feature)-[:MEASURED_ON]->(t:Tool)",
                "WHERE l IS fail_lot",
                "YIELD KEY l, KEY t",
                "",
                "CREATE RULE impacts_tool AS",
                "MATCH (l:Lot)-[:OBSERVED_EXCURSION]->(f:Feature)-[:MEASURED_ON]->(t:Tool)",
                "WHERE l IS fail_lot",
                "DERIVE (l)-[:IMPACTS_TOOL]->(t)",
                "",
                "QUERY fail_tool_excursion WHERE l = l RETURN l.lot_id AS lot_id, t.tool_id AS tool_id, t.module AS module",
                "DERIVE impacts_tool",
                "MATCH (l:Lot)-[:IMPACTS_TOOL]->(t:Tool)",
                "RETURN t.tool_id AS tool, t.module AS module, count(DISTINCT l) AS impacted_fail_lots",
                "ORDER BY impacted_fail_lots DESC, tool",
                "LIMIT 10",
                "'''",
                "",
                "baseline_out = session.locy_with(program_baseline).with_config({",
                "    'max_iterations': 300,",
                "    'timeout': 60.0,",
                "    'max_abduce_candidates': 40,",
                "    'max_abduce_results': 10,",
                "}).run()",
                "",
                "# Persist DERIVE edges to graph",
                "tx = session.tx()",
                "tx.apply(baseline_out.derived_fact_set)",
                "tx.commit()",
                "",
                "stats = baseline_out.stats",
                "print('Iterations:', stats.total_iterations)",
                "print('Strata:', stats.strata_evaluated)",
                "print('Queries executed:', stats.queries_executed)",
                "",
                "hot_tool_rows = []",
                "for i, cmd in enumerate(baseline_out.command_results, start=1):",
                '    print(f"\\nCommand #{i}:", cmd.command_type)',
                "    if cmd.command_type in ('query', 'cypher'):",
                "        rows = cmd.rows",
                "        print('rows:', len(rows))",
                "        pprint(rows[:5])",
                "    elif cmd.command_type == 'derive':",
                "        print('affected:', cmd.affected)",
                "    if cmd.command_type == 'cypher':",
                "        hot_tool_rows = cmd.rows",
                "",
                "if not hot_tool_rows:",
                "    raise RuntimeError('Expected hotspot tool rows from baseline cypher ranking')",
                "",
                "hot_tool = hot_tool_rows[0]['tool']",
                "print('\\nSelected hotspot tool for scenario analysis:', hot_tool)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 6) `EXPLAIN RULE` for a Concrete Failed Lot",
                "",
                "What this does:",
                "Builds a derivation tree for one failed lot so readers can see why it satisfied a target rule.",
                "",
                "What to expect:",
                "A tree-like printout with rule name, clause index, and bindings.",
                "",
                "How to read it:",
                "Parents are conclusions; children are supporting premises.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "focus_lot = focus_fail_ids[0]",
                "",
                "program_explain = f'''",
                "CREATE RULE fail_lot AS",
                "MATCH (l:Lot)",
                "WHERE l.yield_outcome = 'FAIL'",
                "YIELD KEY l",
                "",
                "CREATE RULE fail_tool_excursion AS",
                "MATCH (l:Lot)-[:OBSERVED_EXCURSION]->(f:Feature)-[:MEASURED_ON]->(t:Tool)",
                "WHERE l IS fail_lot",
                "YIELD KEY l, KEY t",
                "",
                "EXPLAIN RULE fail_tool_excursion WHERE l.lot_id = '{focus_lot}' RETURN t",
                "'''",
                "",
                "explain_out = session.locy(program_explain)",
                "explain_cmd = next(cmd for cmd in explain_out.command_results if cmd.command_type == 'explain')",
                "tree = explain_cmd.tree",
                "",
                "def _print_tree(node, depth=0, max_depth=4):",
                "    indent = '  ' * depth",
                "    rule = node.get('rule')",
                "    clause = node.get('clause_index')",
                "    bindings = node.get('bindings', {})",
                '    print(f"{indent}- rule={rule}, clause={clause}, bindings={bindings}")',
                "    if depth >= max_depth:",
                "        return",
                "    for child in node.get('children', []):",
                "        _print_tree(child, depth + 1, max_depth=max_depth)",
                "",
                "print('Explain tree for lot:', focus_lot)",
                "_print_tree(tree)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 7) Counterfactual Containment with `ASSUME`",
                "",
                "What this does:",
                "Applies a hypothetical hold on the hotspot tool, then compares contained vs residual failed lots.",
                "",
                "What to expect:",
                "One `assume` result block listing failed lots that become contained in the hypothetical state.",
                "",
                "How to read it:",
                "Residual count is computed as `total_fail_lots - contained_fail_lots`.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "program_assume = f'''",
                "ASSUME {{",
                "  MATCH (l:Lot)-[:IMPACTS_TOOL]->(t:Tool {{tool_id: '{hot_tool}'}})",
                "  CREATE (l)-[:CONTAINED_BY]->(t)",
                "}} THEN {{",
                "  MATCH (l:Lot {{yield_outcome: 'FAIL'}})-[:CONTAINED_BY]->(t:Tool)",
                "  RETURN l.lot_id AS lot_id, t.tool_id AS tool_id",
                "}}",
                "'''",
                "",
                "assume_out = session.locy(program_assume)",
                "assume_cmd = next(cmd for cmd in assume_out.command_results if cmd.command_type == 'assume')",
                "contained_rows = assume_cmd.rows",
                "contained_lot_ids = sorted({row['lot_id'] for row in contained_rows})",
                "",
                "total_fail_lots = sum(1 for row in focus_lots if row['yield_outcome'] == 'FAIL')",
                "contained_fail_lots = len(contained_lot_ids)",
                "residual_fail_lots = total_fail_lots - contained_fail_lots",
                "abduce_target_lot = contained_lot_ids[0] if contained_lot_ids else focus_lot",
                "",
                "print('Total fail lots in cohort:', total_fail_lots)",
                "print('Contained fail lots under assumption:', contained_fail_lots)",
                "print('Residual fail lots under assumption:', residual_fail_lots)",
                "print('ABDUCE target lot:', abduce_target_lot)",
                "print('\\nContained sample:')",
                "pprint(contained_rows[:10])",
                "",
                'rollback_check = session.query("MATCH (:Lot)-[r:CONTAINED_BY]->(:Tool) RETURN count(r) AS c")',
                "print('\\nRollback check (should be 0):', rollback_check[0]['c'])",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 8) Minimal Change Search with `ABDUCE`",
                "",
                "What this does:",
                "Asks: what minimal change would make a contained failed lot no longer satisfy the hotspot quarantine rule?",
                "",
                "What to expect:",
                "An `abduce` result with candidate modifications (`remove_edge`, `change_property`, etc.).",
                "",
                "How to read it:",
                "`validated=true` candidates satisfy the abductive goal in hypothetical validation.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "program_abduce = f'''",
                "CREATE RULE needs_quarantine AS",
                "MATCH (l:Lot)-[:OBSERVED_EXCURSION]->(:Feature)-[:MEASURED_ON]->(t:Tool)",
                "WHERE l.yield_outcome = 'FAIL', t.tool_id = '{hot_tool}'",
                "YIELD KEY l",
                "",
                "ABDUCE NOT needs_quarantine WHERE l.lot_id = '{abduce_target_lot}' RETURN l",
                "'''",
                "",
                "abduce_out = session.locy_with(program_abduce).with_config({",
                "    'max_abduce_candidates': 120,",
                "    'max_abduce_results': 12,",
                "    'timeout': 60.0,",
                "}).run()",
                "abduce_cmd = next(cmd for cmd in abduce_out.command_results if cmd.command_type == 'abduce')",
                "mods = abduce_cmd.modifications",
                "",
                "print('ABDUCE target lot:', abduce_target_lot)",
                "print('Abduced modifications:', len(mods))",
                "for i, item in enumerate(mods[:8], start=1):",
                '    print(f"\\nCandidate #{i}")',
                "    pprint(item)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 9) What To Expect",
                "",
                "- Baseline ranking should surface one or more dominant tools for failed-lot excursions.",
                "- `ASSUME` should contain at least one failed lot for the selected hotspot tool.",
                "- Residual failed lots should be lower than total failed lots.",
                "- `ABDUCE NOT` should return at least one validated modification candidate.",
                "- `EXPLAIN RULE` should show a non-empty derivation tree for the focus failed lot.",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 10) Build-Time Assertions",
                "",
                "These assertions make the notebook self-validating in CI/docs builds.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "assert hot_tool_rows, 'Expected non-empty hotspot ranking output'",
                "assert total_fail_lots > 0, 'Expected at least one failed lot'",
                "assert contained_fail_lots > 0, 'Expected ASSUME to contain at least one failed lot'",
                "assert residual_fail_lots < total_fail_lots, 'Expected residual fail lots to decrease under assumption'",
                "assert mods, 'Expected ABDUCE to produce modification candidates'",
                "assert any(item.get('validated') for item in mods), 'Expected at least one validated ABDUCE candidate'",
                "assert tree.get('children'), 'Expected EXPLAIN RULE tree to include child derivations'",
                "print('Notebook assertions passed.')",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 11) Cleanup",
                "",
                "Deletes the temporary on-disk database used for this notebook run.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "shutil.rmtree(DB_DIR, ignore_errors=True)",
                "print('Cleaned up', DB_DIR)",
            ],
        )
    )

    return _create_notebook(cells)


def _check(path: Path, expected: str) -> int:
    actual = path.read_text(encoding="utf-8") if path.exists() else ""
    if actual == expected:
        print("Notebook is up to date.")
        return 0

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
    for line in diff[:120]:
        print(line)
    if len(diff) > 120:
        print("... diff truncated ...")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Check drift without writing."
    )
    args = parser.parse_args(argv)

    notebook = _build_notebook()
    rendered = _render_json(notebook)
    if args.check:
        return _check(NOTEBOOK_PATH, rendered)

    NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
    NOTEBOOK_PATH.write_text(rendered, encoding="utf-8")
    print(f"generated {NOTEBOOK_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
