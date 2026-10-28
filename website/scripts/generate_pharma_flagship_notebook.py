#!/usr/bin/env python3
"""Generate the flagship Locy pharma batch genealogy notebook."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


NOTEBOOK_PATH = Path("website/docs/examples/python/locy_pharma_batch_genealogy.ipynb")


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
    key = "python:locy_pharma_batch_genealogy"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "# Locy Flagship #2: Pharma Batch Genealogy Decisioning",
                "",
                "This notebook uses **real pharma process + laboratory data** (Figshare / Scientific Data) and demonstrates:",
                "",
                "- `ALONG`: carry batch risk through campaign genealogy paths.",
                "- `FOLD`: aggregate impact across derived paths.",
                "- `BEST BY`: choose intervention strategies by **risk first, cost second**.",
                "- `DERIVE`: materialize propagation edges.",
                "- `ASSUME`: evaluate temporary containment scenarios.",
                "- `ABDUCE`: search minimal changes that alter decisions.",
                "- `EXPLAIN RULE`: inspect derivation evidence.",
                "",
                "It is schema-first (recommended) and written for both first-time and advanced Locy readers.",
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
                "- Every code section includes intent and expected outputs.",
                "- Data is a deterministic focus slice for stable docs/CI runtime.",
                "- The flow is: load facts -> derive risk paths -> optimize decisions -> simulate and explain.",
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
                "Load helpers, locate prepared pharma data, and create an isolated temporary database.",
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
                "import os",
                "import shutil",
                "import tempfile",
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
                "def _f(value: str) -> float:",
                "    return float(value) if value not in ('', None) else 0.0",
                "",
                "def _norm_key(key: object) -> str:",
                "    s = str(key)",
                "    if s.startswith('Variable(\"') and s.endswith('\")'):",
                "        return s[len('Variable(\"'):-2]",
                "    return s",
                "",
                "def _norm_rows(rows: list[dict[object, object]]) -> list[dict[str, object]]:",
                "    return [{_norm_key(k): v for k, v in row.items()} for row in rows]",
                "",
                "_default_candidates = [",
                "    Path('docs/examples/data/locy_pharma_batch_genealogy'),",
                "    Path('website/docs/examples/data/locy_pharma_batch_genealogy'),",
                "    Path('examples/data/locy_pharma_batch_genealogy'),",
                "    Path('../data/locy_pharma_batch_genealogy'),",
                "]",
                "if 'LOCY_DATA_DIR' in os.environ:",
                "    DATA_DIR = Path(os.environ['LOCY_DATA_DIR']).resolve()",
                "else:",
                "    DATA_DIR = next(",
                "        (p.resolve() for p in _default_candidates if (p / 'pharma_batches.csv').exists()),",
                "        _default_candidates[0].resolve(),",
                "    )",
                "if not (DATA_DIR / 'pharma_batches.csv').exists():",
                "    raise FileNotFoundError(",
                "        'Expected data under docs/examples/data/locy_pharma_batch_genealogy. '",
                "        'Run from website/ (or repo root) or set LOCY_DATA_DIR.'",
                "    )",
                "DB_DIR = tempfile.mkdtemp(prefix='uni_locy_pharma_')",
                "db = uni_db.Uni.open(DB_DIR)",
                "session = db.session()",
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
                "## 2) Load Real Data and Build a Focus Cohort",
                "",
                "What this does:",
                "Loads batches, material lots, campaign links, and intervention plans; selects a deterministic cohort for fast execution.",
                "",
                "What to expect:",
                "Counts for focus batches, deviation batches, material lots, genealogy edges, and action plans.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "batches = _read_csv(DATA_DIR / 'pharma_batches.csv')",
                "materials = _read_csv(DATA_DIR / 'pharma_material_lots.csv')",
                "usage_edges = _read_csv(DATA_DIR / 'pharma_usage_edges.csv')",
                "campaign_edges = _read_csv(DATA_DIR / 'pharma_campaign_edges.csv')",
                "actions = _read_csv(DATA_DIR / 'pharma_action_plans.csv')",
                "notebook_cases = _read_csv(DATA_DIR / 'pharma_notebook_cases.csv')",
                "",
                "focus_deviation_ids = [r['batch_id'] for r in notebook_cases[:24]]",
                "in_spec_ids = [r['batch_id'] for r in batches if r['quality_state'] == 'IN_SPEC'][:72]",
                "focus_ids = set(focus_deviation_ids + in_spec_ids)",
                "",
                "focus_batches = [r for r in batches if r['batch_id'] in focus_ids]",
                "focus_usage = [r for r in usage_edges if r['batch_id'] in focus_ids]",
                "material_ids = {r['material_lot_id'] for r in focus_usage}",
                "focus_materials = [r for r in materials if r['material_lot_id'] in material_ids]",
                "focus_campaign = [",
                "    r for r in campaign_edges",
                "    if r['src_batch_id'] in focus_ids and r['dst_batch_id'] in focus_ids",
                "]",
                "focus_actions = [r for r in actions if r['batch_id'] in focus_ids]",
                "",
                "print('focus batches:', len(focus_batches))",
                "print('focus deviation batches:', sum(1 for r in focus_batches if r['quality_state'] == 'DEVIATION'))",
                "print('focus material lots:', len(focus_materials))",
                "print('focus usage edges:', len(focus_usage))",
                "print('focus campaign edges:', len(focus_campaign))",
                "print('focus action plans:', len(focus_actions))",
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
                "Defines typed nodes and relationships before ingest.",
                "",
                "What to expect:",
                "A single `Schema created` confirmation.",
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
                "    .label('Batch')",
                "        .property('batch_id', 'string')",
                "        .property('product_code', 'string')",
                "        .property('quality_state', 'string')",
                "        .property('deviation_score', 'float64')",
                "        .property('process_risk', 'float64')",
                "        .property('dissolution_min', 'float64')",
                "        .property('residual_solvent', 'float64')",
                "        .property('impurities_total', 'float64')",
                "    .done()",
                "    .label('MaterialLot')",
                "        .property('material_lot_id', 'string')",
                "        .property('material_type', 'string')",
                "        .property('intrinsic_risk', 'float64')",
                "        .property('batches_seen', 'int64')",
                "    .done()",
                "    .label('ActionPlan')",
                "        .property('action_id', 'string')",
                "        .property('batch_id', 'string')",
                "        .property('action_type', 'string')",
                "        .property('cost_index', 'float64')",
                "        .property('downtime_hours', 'float64')",
                "        .property('mitigation_factor', 'float64')",
                "    .done()",
                "    .edge_type('USED_IN', ['MaterialLot'], ['Batch'])",
                "        .property('criticality_weight', 'float64')",
                "    .done()",
                "    .edge_type('NEXT_BATCH', ['Batch'], ['Batch'])",
                "        .property('carry_risk', 'float64')",
                "    .done()",
                "    .edge_type('CANDIDATE_FOR', ['Batch'], ['ActionPlan'])",
                "    .done()",
                "    .edge_type('PROPAGATES_TO', ['Batch'], ['Batch'])",
                "    .done()",
                "    .edge_type('CONTAINED_BY', ['Batch'], ['ActionPlan'])",
                "    .done()",
                "    .apply()",
                ")",
                "print('Schema created')",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 4) Ingest the Pharma Genealogy Graph",
                "",
                "What this does:",
                "Creates `Batch`, `MaterialLot`, and `ActionPlan` nodes, then connects material usage and campaign carryover edges.",
                "",
                "What to expect:",
                "Graph counts for key node and edge types.",
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
                "for row in focus_batches:",
                "    tx.execute(",
                "        f\"CREATE (:Batch {{batch_id: '{_esc(row['batch_id'])}', product_code: '{_esc(row['product_code'])}', \"",
                "        f\"quality_state: '{_esc(row['quality_state'])}', deviation_score: {_f(row['deviation_score'])}, \"",
                "        f\"process_risk: {_f(row['process_risk'])}, dissolution_min: {_f(row['dissolution_min'])}, \"",
                "        f\"residual_solvent: {_f(row['residual_solvent'])}, impurities_total: {_f(row['impurities_total'])}}})\"",
                "    )",
                "",
                "for row in focus_materials:",
                "    tx.execute(",
                "        f\"CREATE (:MaterialLot {{material_lot_id: '{_esc(row['material_lot_id'])}', material_type: '{_esc(row['material_type'])}', \"",
                "        f\"intrinsic_risk: {_f(row['intrinsic_risk'])}, batches_seen: {int(float(row['batches_seen']))}}})\"",
                "    )",
                "",
                "for row in focus_actions:",
                "    tx.execute(",
                "        f\"CREATE (:ActionPlan {{action_id: '{_esc(row['action_id'])}', batch_id: '{_esc(row['batch_id'])}', \"",
                "        f\"action_type: '{_esc(row['action_type'])}', cost_index: {_f(row['cost_index'])}, \"",
                "        f\"downtime_hours: {_f(row['downtime_hours'])}, mitigation_factor: {_f(row['mitigation_factor'])}}})\"",
                "    )",
                "",
                "for row in focus_usage:",
                "    tx.execute(",
                "        f\"MATCH (m:MaterialLot {{material_lot_id: '{_esc(row['material_lot_id'])}'}}), \"",
                "        f\"(b:Batch {{batch_id: '{_esc(row['batch_id'])}'}}) \"",
                "        f\"CREATE (m)-[:USED_IN {{criticality_weight: {_f(row['criticality_weight'])}}}]->(b)\"",
                "    )",
                "",
                "for row in focus_campaign:",
                "    tx.execute(",
                "        f\"MATCH (s:Batch {{batch_id: '{_esc(row['src_batch_id'])}'}}), \"",
                "        f\"(d:Batch {{batch_id: '{_esc(row['dst_batch_id'])}'}}) \"",
                "        f\"CREATE (s)-[:NEXT_BATCH {{carry_risk: {_f(row['carry_risk'])}}}]->(d)\"",
                "    )",
                "",
                "for row in focus_actions:",
                "    tx.execute(",
                "        f\"MATCH (b:Batch {{batch_id: '{_esc(row['batch_id'])}'}}), \"",
                "        f\"(a:ActionPlan {{action_id: '{_esc(row['action_id'])}'}}) \"",
                '        "CREATE (b)-[:CANDIDATE_FOR]->(a)"',
                "    )",
                "",
                "tx.commit()",
                "",
                'counts = session.query("""',
                "MATCH (b:Batch) WITH count(*) AS batches",
                "MATCH (m:MaterialLot) WITH batches, count(*) AS materials",
                "MATCH ()-[u:USED_IN]->() WITH batches, materials, count(*) AS usage_edges",
                "MATCH ()-[n:NEXT_BATCH]->() WITH batches, materials, usage_edges, count(*) AS campaign_edges",
                "MATCH (a:ActionPlan)",
                "RETURN batches, materials, usage_edges, campaign_edges, count(*) AS action_plans",
                '""")',
                "print('Graph counts:')",
                "pprint(counts[0])",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 5) Baseline Locy Program (`DERIVE` + `ALONG` + `FOLD` + `BEST BY`)",
                "",
                "What this does:",
                "Builds recursive campaign propagation, aggregates impact with `FOLD`, materializes propagation edges with `DERIVE`, and chooses best action per deviating batch with `BEST BY`.",
                "",
                "What to expect:",
                "- propagation path rows (`source_batch`, `impacted_batch`, `path_risk`, `hops`)",
                "- `derive` affected count",
                "- best action rows (`batch_id`, `action_type`, `residual_risk`, `plan_cost`)",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "program_baseline = r'''",
                "CREATE RULE deviation_batch AS",
                "MATCH (b:Batch)",
                "WHERE b.quality_state = 'DEVIATION'",
                "YIELD KEY b",
                "",
                "CREATE RULE propagation_path AS",
                "MATCH (src:Batch)-[e:NEXT_BATCH]->(dst:Batch)",
                "WHERE src IS deviation_batch",
                "ALONG path_risk = src.process_risk + e.carry_risk, hops = 1",
                "BEST BY path_risk DESC, hops ASC",
                "YIELD KEY src, KEY dst, path_risk, hops",
                "",
                "CREATE RULE propagation_path AS",
                "MATCH (src:Batch)-[e:NEXT_BATCH]->(mid:Batch)",
                "WHERE mid IS propagation_path TO dst",
                "ALONG path_risk = prev.path_risk + e.carry_risk, hops = prev.hops + 1",
                "BEST BY path_risk DESC, hops ASC",
                "YIELD KEY src, KEY dst, path_risk, hops",
                "",
                "CREATE RULE propagation_summary AS",
                "MATCH (src:Batch)",
                "WHERE src IS propagation_path TO dst",
                "FOLD impacted_batches = COUNT(dst), total_path_risk = SUM(path_risk), max_hops = MAX(hops)",
                "YIELD KEY src, impacted_batches, total_path_risk, max_hops",
                "",
                "CREATE RULE derive_propagation AS",
                "MATCH (src:Batch)-[:NEXT_BATCH]->(dst:Batch)",
                "WHERE src IS deviation_batch",
                "DERIVE (src)-[:PROPAGATES_TO]->(dst)",
                "",
                "CREATE RULE best_action AS",
                "MATCH (b:Batch)-[:CANDIDATE_FOR]->(a:ActionPlan)",
                "WHERE b IS deviation_batch",
                "ALONG residual_risk = b.process_risk * (1.0 - a.mitigation_factor), plan_cost = a.cost_index",
                "BEST BY residual_risk ASC, plan_cost ASC",
                "YIELD KEY b, a, residual_risk, plan_cost",
                "",
                "QUERY propagation_path WHERE src = src RETURN src.batch_id AS source_batch, dst.batch_id AS impacted_batch, path_risk, hops",
                "DERIVE derive_propagation",
                "QUERY best_action WHERE b = b RETURN b.batch_id AS batch_id, a.action_type AS action_type, residual_risk, plan_cost",
                "'''",
                "",
                "baseline_out = session.locy_with(program_baseline).with_config({",
                "    'max_iterations': 400,",
                "    'timeout': 60.0,",
                "    'max_abduce_candidates': 80,",
                "    'max_abduce_results': 12,",
                "}).run()",
                "",
                "# Persist DERIVE edges to graph",
                "tx = session.tx()",
                "tx.apply(baseline_out.derived_fact_set)",
                "tx.commit()",
                "stats = baseline_out.stats",
                "print('Iterations:', stats.total_iterations)",
                "print('Strata:', stats.strata_evaluated)",
                "print('Queries executed:', stats.queries_executed)",
                "",
                "propagation_rows = []",
                "best_plan_rows = []",
                "propagation_path_rows = []",
                "for i, cmd in enumerate(baseline_out.command_results, start=1):",
                '    print(f"\\nCommand #{i}:", cmd.command_type)',
                "    if cmd.command_type in ('query', 'cypher'):",
                "        rows = _norm_rows(cmd.rows)",
                "        print('rows:', len(rows))",
                "        pprint(rows[:5])",
                "        if rows and 'impacted_batch' in rows[0]:",
                "            propagation_path_rows = rows",
                "        if rows and 'action_type' in rows[0]:",
                "            best_plan_rows = rows",
                "    elif cmd.command_type == 'derive':",
                "        print('affected:', cmd.affected)",
                "",
                "source_rollup = {}",
                "for row in propagation_path_rows:",
                "    source = str(row.get('source_batch', ''))",
                "    impacted = str(row.get('impacted_batch', ''))",
                "    info = source_rollup.setdefault(source, {'source_batch': source, 'impacted': set(), 'downstream_risk': 0.0, 'max_hops': 0})",
                "    if impacted:",
                "        info['impacted'].add(impacted)",
                "    info['downstream_risk'] += _f(str(row.get('path_risk', '0')))",
                "    info['max_hops'] = max(int(info['max_hops']), int(_f(str(row.get('hops', '0')))))",
                "",
                "propagation_rows = [",
                "    {",
                "        'source_batch': v['source_batch'],",
                "        'impacted_batches': len(v['impacted']),",
                "        'downstream_risk': v['downstream_risk'],",
                "        'max_hops': v['max_hops'],",
                "    }",
                "    for v in source_rollup.values()",
                "]",
                "propagation_rows = sorted(",
                "    propagation_rows,",
                "    key=lambda r: (-int(r.get('impacted_batches', 0)), -_f(str(r.get('downstream_risk', '0'))), str(r.get('source_batch', ''))),",
                ")",
                "best_plan_rows = sorted(",
                "    best_plan_rows,",
                "    key=lambda r: (_f(str(r.get('residual_risk', '0'))), _f(str(r.get('plan_cost', '0'))), str(r.get('batch_id', ''))),",
                ")",
                "",
                "if not propagation_rows:",
                "    raise RuntimeError('Expected non-empty propagation summary rows')",
                "if not best_plan_rows:",
                "    raise RuntimeError('Expected non-empty best action rows')",
                "",
                "focus_source_batch = str(propagation_rows[0]['source_batch'])",
                "focus_plan_batch = str(best_plan_rows[0]['batch_id'])",
                "print('\\nTop propagation source batch:', focus_source_batch)",
                "print('Top selected action batch:', focus_plan_batch)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 6) Explain One Propagation Path (`EXPLAIN RULE`)",
                "",
                "What this does:",
                "Produces a derivation tree for recursive propagation from one high-risk source batch.",
                "",
                "What to expect:",
                "A non-empty tree with rule/clause and supporting child derivations.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "program_explain = f'''",
                "CREATE RULE deviation_batch AS",
                "MATCH (b:Batch)",
                "WHERE b.quality_state = 'DEVIATION'",
                "YIELD KEY b",
                "",
                "CREATE RULE propagation_path AS",
                "MATCH (src:Batch)-[e:NEXT_BATCH]->(dst:Batch)",
                "WHERE src IS deviation_batch",
                "ALONG path_risk = src.process_risk + e.carry_risk, hops = 1",
                "BEST BY path_risk DESC, hops ASC",
                "YIELD KEY src, KEY dst, path_risk, hops",
                "",
                "CREATE RULE propagation_path AS",
                "MATCH (src:Batch)-[e:NEXT_BATCH]->(mid:Batch)",
                "WHERE mid IS propagation_path TO dst",
                "ALONG path_risk = prev.path_risk + e.carry_risk, hops = prev.hops + 1",
                "BEST BY path_risk DESC, hops ASC",
                "YIELD KEY src, KEY dst, path_risk, hops",
                "",
                "EXPLAIN RULE propagation_path WHERE src.batch_id = '{focus_source_batch}' RETURN dst",
                "'''",
                "",
                "explain_out = session.locy(program_explain)",
                "explain_cmd = next(cmd for cmd in explain_out.command_results if cmd.command_type == 'explain')",
                "tree = explain_cmd.tree",
                "",
                "def _print_tree(node, depth=0, max_depth=3, max_children=3):",
                "    indent = '  ' * depth",
                "    print(f\"{indent}- rule={node.get('rule')}, clause={node.get('clause_index')}, bindings={node.get('bindings', {})}\")",
                "    if depth >= max_depth:",
                "        return",
                "    children = node.get('children', [])",
                "    for child in children[:max_children]:",
                "        _print_tree(child, depth + 1, max_depth=max_depth, max_children=max_children)",
                "    if len(children) > max_children:",
                '        print(f"{indent}  ... {len(children) - max_children} more child derivations")',
                "",
                "print('Explain tree for source batch:', focus_source_batch)",
                "_print_tree(tree)",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 7) Counterfactual Containment (`ASSUME`)",
                "",
                "What this does:",
                "Temporarily applies deep-clean containment for high-risk deviating batches and compares contained vs residual deviations.",
                "",
                "What to expect:",
                "Contained batch rows from the hypothetical world; rollback check should show zero persisted edges.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "assume_program = '''",
                "ASSUME {",
                "  MATCH (b:Batch {quality_state: 'DEVIATION'})-[:CANDIDATE_FOR]->(a:ActionPlan {action_type: 'deep_clean_hold'})",
                "  WHERE b.process_risk >= 0.55",
                "  CREATE (b)-[:CONTAINED_BY]->(a)",
                "} THEN {",
                "  MATCH (b:Batch {quality_state: 'DEVIATION'})-[:CONTAINED_BY]->(a:ActionPlan)",
                "  RETURN b.batch_id AS batch_id, a.action_type AS action_type",
                "}",
                "'''",
                "",
                "assume_out = session.locy(assume_program)",
                "assume_cmd = next(cmd for cmd in assume_out.command_results if cmd.command_type == 'assume')",
                "contained_rows = assume_cmd.rows",
                "contained_batch_ids = sorted({r['batch_id'] for r in contained_rows})",
                "",
                "total_deviating_batches = sum(1 for r in focus_batches if r['quality_state'] == 'DEVIATION')",
                "contained_deviations = len(contained_batch_ids)",
                "residual_deviations = total_deviating_batches - contained_deviations",
                "abduce_target_batch = contained_batch_ids[0] if contained_batch_ids else focus_plan_batch",
                "",
                "print('Total deviation batches:', total_deviating_batches)",
                "print('Contained deviation batches:', contained_deviations)",
                "print('Residual deviation batches:', residual_deviations)",
                "print('ABDUCE target batch:', abduce_target_batch)",
                "print('\\nContained sample:')",
                "pprint(contained_rows[:10])",
                "",
                'rollback_check = session.query("MATCH (:Batch)-[r:CONTAINED_BY]->(:ActionPlan) RETURN count(r) AS c")',
                "print('\\nRollback check (should be 0):', rollback_check[0]['c'])",
            ],
        )
    )

    cells.append(
        _md_cell(
            key,
            len(cells),
            [
                "## 8) Minimal Change Search (`ABDUCE`)",
                "",
                "What this does:",
                "Finds minimal graph/program changes that would remove a target batch from the deep-clean requirement.",
                "",
                "What to expect:",
                "At least one validated candidate modification.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "program_abduce = f'''",
                "CREATE RULE needs_deep_clean AS",
                "MATCH (b:Batch)-[:CANDIDATE_FOR]->(a:ActionPlan)",
                "WHERE b.quality_state = 'DEVIATION', a.action_type = 'deep_clean_hold'",
                "YIELD KEY b",
                "",
                "ABDUCE NOT needs_deep_clean WHERE b.batch_id = '{abduce_target_batch}' RETURN b",
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
                "print('ABDUCE target batch:', abduce_target_batch)",
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
                "- Propagation summary should show non-empty impacted-batch counts from `ALONG` recursion.",
                "- Best action rows should pick one action per batch using `BEST BY` (risk first, cost second).",
                "- `ASSUME` should contain at least one deviation batch.",
                "- Residual deviation count should be lower than total deviation count.",
                "- `ABDUCE NOT` should return at least one validated candidate.",
                "- `EXPLAIN RULE` should return a derivation tree with children.",
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
                "These assertions keep the notebook self-validating in CI/docs builds.",
            ],
        )
    )
    cells.append(
        _code_cell(
            key,
            len(cells),
            [
                "assert propagation_rows, 'Expected non-empty propagation rows'",
                "assert best_plan_rows, 'Expected non-empty best action rows'",
                "assert total_deviating_batches > 0, 'Expected deviation batches in focus cohort'",
                "assert contained_deviations > 0, 'Expected ASSUME containment to affect at least one batch'",
                "assert residual_deviations < total_deviating_batches, 'Expected residual deviations to decrease'",
                "assert mods, 'Expected ABDUCE to produce modifications'",
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
                "Deletes the temporary on-disk database for this notebook run.",
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
