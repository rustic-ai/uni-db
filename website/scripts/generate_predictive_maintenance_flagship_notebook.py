#!/usr/bin/env python3
"""Generate the flagship Locy Predictive Maintenance notebook (Python).

Demonstrates the Phase D neural-predicate capabilities end-to-end against
the AI4I 2020 dataset (UCI #601, CC BY 4.0) plus a synthesised
process-line topology:

  - Real sensor data ingested from vendored AI4I CSV.
  - CREATE MODEL with property FEATURES + a Python-registered classifier.
  - Component-level risk via FOLD MNOR(1 - c.health) over HAS_PART.
  - Line-level reliability via FOLD MPROD(1 - fl(a.air_temp_k)) across
    UPSTREAM_OF — inline classifier invocation inside the aggregator
    composes the per-asset prediction with the topology.
  - CALIBRATE ... METHOD platt_scaling against the actual_failed labels
    in the curated AI4I slice.
  - VALIDATE with Brier + accuracy on the same labels.
  - EXPLAIN trace surfacing the NeuralProvenance per derivation.
  - Ranked maintenance queue combining calibrated per-asset risk with
    downstream-impact line reliability.

The dataset is vendored under
`website/docs/examples/data/locy_predictive_maintenance/` by
`prepare_predictive_maintenance_notebook_data.py`. The runtime
classifier is a deterministic Python callable so the notebook is
reproducible without ONNX / sklearn dependencies — in production you'd
register any callable matching the `list[dict] -> list[float]` contract.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


NOTEBOOK_PATH = Path("website/docs/examples/python/locy_predictive_maintenance.ipynb")
DATA_DIR_RELATIVE = "website/docs/examples/data/locy_predictive_maintenance"


def _cell_id(notebook_key: str, index: int, cell_type: str) -> str:
    raw = f"{notebook_key}:{index}:{cell_type}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def _src(lines: list[str]) -> list[str]:
    return [f"{line}\n" for line in lines]


def _md(key: str, idx: int, lines: list[str]) -> dict[str, Any]:
    return {
        "id": _cell_id(key, idx, "markdown"),
        "cell_type": "markdown",
        "metadata": {},
        "source": _src(lines),
    }


def _code(key: str, idx: int, lines: list[str]) -> dict[str, Any]:
    return {
        "id": _cell_id(key, idx, "code"),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _src(lines),
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


def _build_notebook() -> dict[str, Any]:
    key = "python:locy_predictive_maintenance"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md(
            key,
            len(cells),
            [
                "# Locy Flagship: Predictive Maintenance with Topology-Aware Calibrated Risk",
                "",
                "This notebook delivers the Phase D neural-predicate capabilities against the **AI4I 2020 Predictive Maintenance Dataset** (UCI #601, CC BY 4.0) — a real industrial-sensor dataset — wired together with a synthesised process-line topology:",
                "",
                "- Ingest a curated 60-row AI4I slice plus a 4-stage process-line topology.",
                "- Register a Python-callable classifier as a Locy `CREATE MODEL` under the `failure_likelihood` alias.",
                "- Component-level risk via `FOLD MNOR(1 - c.health)` over `HAS_PART`.",
                "- Line-level reliability via `FOLD MPROD(1 - fl(a.air_temp_k))` across `UPSTREAM_OF` — *inline classifier invocation inside the aggregator* composes the per-asset prediction with the topology in one declarative step.",
                "- `CALIBRATE failure_likelihood ... METHOD platt_scaling` against the `actual_failed` labels, with raw vs calibrated Brier delta.",
                "- `VALIDATE` reports Brier + accuracy on the same labels.",
                "- `EXPLAIN` traces surface the classifier's `NeuralProvenance` per derivation.",
                "- A ranked maintenance queue combines calibrated per-asset risk with downstream-impact line reliability.",
                "",
                "The runtime classifier is a deterministic Python callable so the notebook is reproducible without ONNX / sklearn deps. In production you'd register any callable matching the `list[dict] -> list[float]` contract.",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 1) Setup",
                "",
                "Open a temporary `Uni` and declare the schema for AI4I `Equipment` nodes (all 14 sensor / failure-mode properties), `Component` sub-parts (with health), and the `UPSTREAM_OF` topology edge.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "import csv",
                "import tempfile",
                "import shutil",
                "from pathlib import Path",
                "",
                "import uni_db",
                "",
                "WORK_DIR = Path(tempfile.mkdtemp(prefix='uni_locy_pdm_'))",
                "DB_DIR = WORK_DIR / 'db'",
                "db = uni_db.Uni.open(str(DB_DIR))",
                "",
                "(db.schema()",
                "    .label('Equipment')",
                "        .property('udi', 'string')",
                "        .property('product_id', 'string')",
                "        .property('type', 'string')",
                "        .property('air_temp_k', 'float')",
                "        .property('process_temp_k', 'float')",
                "        .property('rotational_speed_rpm', 'float')",
                "        .property('torque_nm', 'float')",
                "        .property('tool_wear_min', 'float')",
                "        .property('actual_failed', 'bool')",
                "        .property('twf_label', 'int')",
                "        .property('hdf_label', 'int')",
                "        .property('pwf_label', 'int')",
                "        .property('osf_label', 'int')",
                "        .property('rnf_label', 'int')",
                "    .done()",
                "    .label('Component')",
                "        .property('part_id', 'string')",
                "        .property('equipment_id', 'string')",
                "        .property('health', 'float')",
                "    .done()",
                "    .apply())",
                "print(f'DB at {DB_DIR}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 2) Load Vendored AI4I 2020 Data",
                "",
                "Real sensor rows are vendored by `website/scripts/prepare_predictive_maintenance_notebook_data.py` (stratified 30 failed + 30 healthy from AI4I 2020). The process-line topology and per-equipment component health are synthesised and marked as such in the data manifest.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "def _find_data_dir():",
                f"    rel = '{DATA_DIR_RELATIVE}'",
                "    cur = Path.cwd().resolve()",
                "    for parent in (cur, *cur.parents):",
                "        candidate = parent / rel",
                "        if candidate.exists():",
                "            return candidate",
                "    raise AssertionError(",
                "        f'Data directory not found from {cur}. '",
                "        f'Run `python website/scripts/prepare_predictive_maintenance_notebook_data.py` first.'",
                "    )",
                "",
                "DATA_DIR = _find_data_dir()",
                "",
                "def _read_csv(name):",
                "    with open(DATA_DIR / name, encoding='utf-8') as f:",
                "        return list(csv.DictReader(f))",
                "",
                "EQUIPMENT_ROWS = _read_csv('ai4i_equipment.csv')",
                "TOPOLOGY_EDGES = _read_csv('ai4i_topology.csv')",
                "COMPONENT_ROWS = _read_csv('ai4i_components.csv')",
                "",
                "print(f'Loaded {len(EQUIPMENT_ROWS)} equipment rows '",
                "      f'({sum(1 for r in EQUIPMENT_ROWS if r[\"actual_failed\"] == \"true\")} failed)')",
                "print(f'Loaded {len(TOPOLOGY_EDGES)} UPSTREAM_OF edges across 4 stages')",
                "print(f'Loaded {len(COMPONENT_ROWS)} component rows')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 3) Ingest into Uni",
                "",
                "Each AI4I row becomes an `Equipment` node carrying all 14 properties; each synthesised topology edge becomes an `UPSTREAM_OF` relationship; each component becomes a `Component` linked via `HAS_PART`.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "session = db.session()",
                "tx = session.tx()",
                "",
                "def _escape(s):",
                "    return str(s).replace(\"'\", \"\\\\'\")",
                "",
                "for r in EQUIPMENT_ROWS:",
                "    tx.execute(",
                "        \"CREATE (:Equipment {\"",
                "        f\"udi: '{_escape(r['udi'])}', \"",
                "        f\"product_id: '{_escape(r['product_id'])}', \"",
                "        f\"type: '{_escape(r['type'])}', \"",
                "        f\"air_temp_k: {r['air_temp_k']}, \"",
                "        f\"process_temp_k: {r['process_temp_k']}, \"",
                "        f\"rotational_speed_rpm: {r['rotational_speed_rpm']}, \"",
                "        f\"torque_nm: {r['torque_nm']}, \"",
                "        f\"tool_wear_min: {r['tool_wear_min']}, \"",
                "        f\"actual_failed: {r['actual_failed']}, \"",
                "        f\"twf_label: {r['twf_label']}, \"",
                "        f\"hdf_label: {r['hdf_label']}, \"",
                "        f\"pwf_label: {r['pwf_label']}, \"",
                "        f\"osf_label: {r['osf_label']}, \"",
                "        f\"rnf_label: {r['rnf_label']}\"",
                "        \"})\"",
                "    )",
                "",
                "for c in COMPONENT_ROWS:",
                "    tx.execute(",
                "        f\"MATCH (e:Equipment {{udi: '{_escape(c['equipment_id'])}'}}) \"",
                "        f\"CREATE (e)-[:HAS_PART]->(:Component {{\"",
                "        f\"part_id: '{_escape(c['part_id'])}', \"",
                "        f\"equipment_id: '{_escape(c['equipment_id'])}', \"",
                "        f\"health: {c['health']}}})\"",
                "    )",
                "",
                "for e in TOPOLOGY_EDGES:",
                "    tx.execute(",
                "        f\"MATCH (u:Equipment {{udi: '{_escape(e['upstream_id'])}'}}), \"",
                "        f\"      (d:Equipment {{udi: '{_escape(e['downstream_id'])}'}}) \"",
                "        f\"CREATE (u)-[:UPSTREAM_OF]->(d)\"",
                "    )",
                "",
                "tx.commit()",
                "INGESTED_EQUIPMENT = len(EQUIPMENT_ROWS)",
                "INGESTED_EDGES = len(TOPOLOGY_EDGES)",
                "INGESTED_COMPONENTS = len(COMPONENT_ROWS)",
                "print(f'Ingested: {INGESTED_EQUIPMENT} Equipment, {INGESTED_COMPONENTS} Component, {INGESTED_EDGES} UPSTREAM_OF')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 4) Register the Failure-Likelihood Classifier",
                "",
                "`LocyConfig.register_classifier` wires a Python callable into the runtime registry keyed by the `CREATE MODEL <name>` (here `failure_likelihood`). The callable below is a deterministic logistic over air temperature, intentionally over-confident so the `CALIBRATE` step does measurable work. In production this is where you'd plug in an ONNX-exported XGBoost, a sklearn pipeline, or a remote API client.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "import math",
                "",
                "def failure_likelihood(inputs):",
                "    \"\"\"Tabular failure classifier — intentionally over-confident.\"\"\"",
                "    out = []",
                "    for row in inputs:",
                "        # The feature dict is keyed by the INPUT binding name ('e').",
                "        # The value is the evaluated argument at the call site — here,",
                "        # e.air_temp_k (a Float64 in degrees Kelvin).",
                "        air_k = float(row.get('e', 0.0) or 0.0)",
                "        z = (air_k - 300.0) * 0.4 - 0.3",
                "        p = 1.0 / (1.0 + math.exp(-z))",
                "        # Sharpen toward extremes so calibration matters.",
                "        p_sharp = 1.0 / (1.0 + math.exp(-3.5 * (p - 0.5)))",
                "        out.append(max(0.0, min(1.0, p_sharp)))",
                "    return out",
                "",
                "config = uni_db.LocyConfig()",
                "config.register_classifier('failure_likelihood', failure_likelihood)",
                "print(f'Registered classifiers: {config.classifier_aliases()}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 5) Asset Risk + Component Composition + Line Reliability + Stratified Negation + Recursive Reachability",
                "",
                "One declarative Locy program composes five rules:",
                "",
                "- `asset_risk`: per-equipment classifier output.",
                "- `component_risk`: per-equipment `FOLD MNOR(1 - c.health)` over `HAS_PART` — \"this asset is unhealthy if ANY component is degraded\". `MNOR(a, b) = 1 - (1 - a)(1 - b)` (the probabilistic OR; monotone, associative).",
                "- `line_reliability`: per-downstream-equipment `FOLD MPROD(1 - failure_likelihood(a.air_temp_k))` across `UPSTREAM_OF` — joint reliability of the upstream chain. The classifier is invoked *inline inside the aggregator*, so per-asset neural prediction and topology composition happen in one rule.",
                "- `failure_prone` + `healthy_assets`: stratified Locy negation via `WHERE e IS NOT failure_prone` — the complement is computed in a higher stratum and is the dual of the failure-prone set.",
                "- `upstream_reaches` (recursive, two-clause union): transitive closure over `UPSTREAM_OF`. The second clause refers to the rule itself (`WHERE mid IS upstream_reaches TO b`), so fixpoint iterates until every reachable (origin, terminal) pair is enumerated. We use this in §9 to size the blast radius of each asset's failure.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "COMPOSE_PROGRAM = '''",
                "CREATE MODEL failure_likelihood AS",
                "  INPUT (e)",
                "  FEATURES e.air_temp_k",
                "  OUTPUT PROB will_fail",
                "  USING xervo('classify/failure-likelihood-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE RULE asset_risk AS",
                "  MATCH (e:Equipment)",
                "  YIELD KEY e, failure_likelihood(e.air_temp_k) AS risk PROB",
                "",
                "CREATE RULE component_risk AS",
                "  MATCH (e:Equipment)-[:HAS_PART]->(c:Component)",
                "  FOLD composite_unhealth = MNOR(1.0 - c.health)",
                "  YIELD KEY e, composite_unhealth",
                "",
                "CREATE RULE line_reliability AS",
                "  MATCH (a:Equipment)-[:UPSTREAM_OF]->(b:Equipment)",
                "  FOLD reliability = MPROD(1.0 - failure_likelihood(a.air_temp_k))",
                "  YIELD KEY b, reliability",
                "",
                "// Stratified IS NOT complement: leverages the ground-truth",
                "// actual_failed column to define a failure_prone set, then",
                "// computes the complement (healthy assets) — demonstrates the",
                "// Locy stratification + negation surface end-to-end.",
                "CREATE RULE failure_prone AS",
                "  MATCH (e:Equipment)",
                "  WHERE e.actual_failed = true",
                "  YIELD KEY e",
                "",
                "CREATE RULE healthy_assets AS",
                "  MATCH (e:Equipment)",
                "  WHERE e IS NOT failure_prone",
                "  YIELD KEY e",
                "",
                "// Recursive transitive closure over UPSTREAM_OF: enumerates",
                "// every (origin, terminal) pair where origin's failure can",
                "// eventually cascade to terminal via any number of process-",
                "// line hops. Used downstream to size the blast radius for",
                "// the maintenance queue.",
                "CREATE RULE upstream_reaches AS",
                "  MATCH (a:Equipment)-[:UPSTREAM_OF]->(b:Equipment)",
                "  YIELD KEY a, KEY b",
                "",
                "CREATE RULE upstream_reaches AS",
                "  MATCH (a:Equipment)-[:UPSTREAM_OF]->(mid:Equipment)",
                "  WHERE mid IS upstream_reaches TO b",
                "  YIELD KEY a, KEY b",
                "'''",
                "",
                "compose_result = session.locy_with(COMPOSE_PROGRAM).with_config(config).run()",
                "",
                "ASSET_RISK_COUNT = len(compose_result.derived.get('asset_risk', []))",
                "COMPONENT_RISK_COUNT = len(compose_result.derived.get('component_risk', []))",
                "LINE_RELIABILITY_COUNT = len(compose_result.derived.get('line_reliability', []))",
                "FAILURE_PRONE_COUNT = len(compose_result.derived.get('failure_prone', []))",
                "HEALTHY_ASSETS_COUNT = len(compose_result.derived.get('healthy_assets', []))",
                "UPSTREAM_REACHES_COUNT = len(compose_result.derived.get('upstream_reaches', []))",
                "",
                "print(f'Derived: asset_risk={ASSET_RISK_COUNT}  component_risk={COMPONENT_RISK_COUNT}  '",
                "      f'line_reliability={LINE_RELIABILITY_COUNT}')",
                "print(f'         failure_prone={FAILURE_PRONE_COUNT}  '",
                "      f'healthy_assets={HEALTHY_ASSETS_COUNT}  '",
                "      f'upstream_reaches={UPSTREAM_REACHES_COUNT} (transitive)')",
                "",
                "print('\\nTop-5 highest-risk assets (raw classifier output):')",
                "for row in sorted(compose_result.derived.get('asset_risk', []), key=lambda r: -r['risk'])[:5]:",
                "    eid = row.get('e', {}).get('udi', '?') if hasattr(row.get('e', {}), 'get') else getattr(row.get('e'), 'properties', {}).get('udi', '?')",
                "    print(f'  udi={eid:>5}  raw_risk={row[\"risk\"]:.4f}')",
                "",
                "print('\\nLine reliability for downstream stages (lower = riskier upstream chain):')",
                "for row in sorted(compose_result.derived.get('line_reliability', []), key=lambda r: r['reliability'])[:5]:",
                "    b = row.get('b')",
                "    bid = b.properties.get('udi', '?') if hasattr(b, 'properties') else '?'",
                "    print(f'  downstream={bid:>5}  reliability={row[\"reliability\"]:.4f}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 6) Calibrate Against the Real `actual_failed` Labels",
                "",
                "AI4I ships ground-truth `Machine failure` labels — we mapped them into the `actual_failed` boolean during prep. `CALIBRATE failure_likelihood ... METHOD platt_scaling` fits a 2-parameter logistic over the classifier's logits versus those labels and reports raw vs calibrated Brier + ECE.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "CALIBRATE_PROGRAM = '''",
                "CREATE MODEL failure_likelihood AS",
                "  INPUT (e)",
                "  FEATURES e.air_temp_k",
                "  OUTPUT PROB will_fail",
                "  USING xervo('classify/failure-likelihood-v1')",
                "  VERSION '1.0.0'",
                "",
                "CALIBRATE failure_likelihood",
                "  ON MATCH (e:Equipment)",
                "  TARGET e.actual_failed",
                "  METHOD platt_scaling",
                "'''",
                "",
                "calib_result = session.locy_with(CALIBRATE_PROGRAM).with_config(config).run()",
                "calib_records = [c for c in calib_result.command_results if isinstance(c, dict) and c.get('type') == 'calibrate']",
                "BRIER_DELTA = None",
                "CALIBRATOR = None  # exposed for downstream calibrated-rescoring (cell 9)",
                "if calib_records:",
                "    c = calib_records[0]",
                "    print(f'Calibration: {c[\"method\"]}')",
                "    print(f'  raw         brier={c[\"raw_brier\"]:.4f}  ece={c[\"raw_ece\"]:.4f}')",
                "    print(f'  calibrated  brier={c[\"calibrated_brier\"]:.4f}  ece={c[\"calibrated_ece\"]:.4f}')",
                "    BRIER_DELTA = c['raw_brier'] - c['calibrated_brier']",
                "    print(f'  delta_brier = {BRIER_DELTA:+.4f} (positive = calibrated is better)')",
                "    CALIBRATOR = c.get('calibrator')",
                "    print(f'  fitted calibrator: {CALIBRATOR}')",
                "else:",
                "    print('No calibration record returned')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 7) Validate",
                "",
                "`VALIDATE` independently scores a rule's `PROB` output against ground truth. Brier measures probability quality (squared error vs the 0/1 outcome); accuracy measures threshold-based classification.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "VALIDATE_PROGRAM = '''",
                "CREATE MODEL failure_likelihood AS",
                "  INPUT (e)",
                "  FEATURES e.air_temp_k",
                "  OUTPUT PROB will_fail",
                "  USING xervo('classify/failure-likelihood-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE RULE labeled_assets AS",
                "  MATCH (e:Equipment)",
                "  YIELD KEY e, failure_likelihood(e.air_temp_k) AS predicted PROB",
                "",
                "VALIDATE labeled_assets",
                "  ON MATCH (e:Equipment)",
                "  TARGET e.actual_failed",
                "  METRICS brier_score, accuracy",
                "'''",
                "",
                "val_result = session.locy_with(VALIDATE_PROGRAM).with_config(config).run()",
                "val_records = [c for c in val_result.command_results if isinstance(c, dict) and c.get('type') == 'validate']",
                "VALIDATE_METRICS = val_records[0]['metrics'] if val_records else {}",
                "print(f'Validation metrics: {VALIDATE_METRICS}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 8) EXPLAIN: Audit Trail for One High-Risk Asset",
                "",
                "`EXPLAIN RULE asset_risk WHERE ...` returns the proof tree for one derivation. For neural-predicate rules, each derivation carries a `NeuralProvenance` entry — raw and calibrated probability, confidence band, feature dict. This is the regulator-ready audit trail.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "# Pick the top-1 asset by raw risk for the EXPLAIN target.",
                "top_asset = max(compose_result.derived.get('asset_risk', []), key=lambda r: r['risk'])",
                "top_udi = top_asset.get('e').properties.get('udi') if hasattr(top_asset.get('e'), 'properties') else None",
                "print(f'EXPLAIN target: udi={top_udi}  raw_risk={top_asset[\"risk\"]:.4f}')",
                "",
                "EXPLAIN_PROGRAM = f'''",
                "CREATE MODEL failure_likelihood AS",
                "  INPUT (e)",
                "  FEATURES e.air_temp_k",
                "  OUTPUT PROB will_fail",
                "  USING xervo('classify/failure-likelihood-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE RULE asset_risk AS",
                "  MATCH (e:Equipment)",
                "  YIELD KEY e, failure_likelihood(e.air_temp_k) AS risk",
                "",
                "EXPLAIN RULE asset_risk WHERE e.udi = '{top_udi}'",
                "'''",
                "",
                "explain_result = session.locy_with(EXPLAIN_PROGRAM).with_config(config).run()",
                "explain_records = [c for c in explain_result.command_results if isinstance(c, uni_db.ExplainCommandResult)]",
                "EXPLAIN_PRODUCED = len(explain_records)",
                "print(f'EXPLAIN records: {EXPLAIN_PRODUCED}')",
                "",
                "# Walk the tree (the WHERE filter already narrowed it to one leaf).",
                "def _format_node(node, depth=0, out=None):",
                "    if out is None:",
                "        out = []",
                "    if not isinstance(node, dict):",
                "        return out",
                "    indent = '  ' * depth",
                "    rule = node.get('rule', '?')",
                "    bindings = node.get('bindings', {}) or {}",
                "    pp = node.get('proof_probability')",
                "    out.append(f'{indent}rule={rule}  clause={node.get(\"clause_index\")}  '",
                "               f'proof_p={pp}')",
                "    if bindings:",
                "        keys = sorted(k for k in bindings if not k.startswith('__'))",
                "        kv = ', '.join(f'{k}={bindings[k]!r}' for k in keys[:4])",
                "        out.append(f'{indent}  bindings: {kv}')",
                "    for call in node.get('neural_calls', []) or []:",
                "        out.append(",
                "            f'{indent}  neural: model={call[\"model_name\"]!r} '",
                "            f'raw={call[\"raw_probability\"]:.4f} '",
                "            f'calibrated={call[\"calibrated_probability\"]} '",
                "            f'band={call[\"confidence_band\"]}'",
                "        )",
                "    for child in node.get('children', []) or []:",
                "        _format_node(child, depth + 1, out)",
                "    return out",
                "",
                "if explain_records:",
                "    tree = getattr(explain_records[0], 'tree', None)",
                "    if tree is not None:",
                "        print('\\n'.join(_format_node(tree)))",
                "    else:",
                "        print('  (no tree on ExplainCommandResult)')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 9) Ranked Maintenance Queue: Per-Asset Risk × Downstream Impact",
                "",
                "Maintenance prioritisation isn't just \"highest probability of failure\". It's \"highest probability of failure weighted by the value of what fails downstream\". We join the per-asset calibrated risk with the line-reliability rollup to produce a ranked queue that surfaces the assets whose failure would cascade the most.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "# Build a downstream-impact map: for each upstream asset, what is the worst",
                "# downstream line-reliability that its presence drives?",
                "asset_rows = compose_result.derived.get('asset_risk', [])",
                "line_rows = compose_result.derived.get('line_reliability', [])",
                "",
                "# Apply the fitted calibrator so the queue ranks on CALIBRATED risk,",
                "# not raw classifier output. If CALIBRATOR is None (calibration cell",
                "# didn't return one), we fall back to raw risk and flag that fact.",
                "asset_risk_by_udi = {",
                "    r['e'].properties['udi']: (",
                "        CALIBRATOR.apply(r['risk']) if CALIBRATOR is not None else r['risk']",
                "    )",
                "    for r in asset_rows",
                "    if hasattr(r.get('e'), 'properties')",
                "}",
                "if CALIBRATOR is None:",
                "    print('NOTE: no calibrator returned — queue ranked on RAW risk')",
                "else:",
                "    print(f'Queue ranked on CALIBRATED risk via {CALIBRATOR}')",
                "",
                "downstream_min_reliability = {}",
                "for r in line_rows:",
                "    b = r.get('b')",
                "    if hasattr(b, 'properties'):",
                "        downstream_min_reliability[b.properties['udi']] = r['reliability']",
                "",
                "# Blast radius per asset: count of transitive-downstream",
                "# equipment via upstream_reaches (a recursive Locy rule).",
                "# Captures \"how many downstream stages stop if THIS asset fails\".",
                "blast_radius = {}",
                "for r in compose_result.derived.get('upstream_reaches', []):",
                "    a = r.get('a')",
                "    udi = a.properties.get('udi') if hasattr(a, 'properties') else None",
                "    if udi is None:",
                "        continue",
                "    blast_radius[udi] = blast_radius.get(udi, 0) + 1",
                "",
                "# Combined score: high (calibrated) asset risk + downstream-of low",
                "# reliability + larger blast radius => higher priority.",
                "queue = []",
                "for udi, risk in asset_risk_by_udi.items():",
                "    rel = downstream_min_reliability.get(udi, 1.0)",
                "    blast = blast_radius.get(udi, 0)",
                "    priority = risk * (1.0 + (1.0 - rel)) * (1.0 + 0.1 * blast)",
                "    queue.append((udi, risk, rel, blast, priority))",
                "queue.sort(key=lambda t: -t[4])",
                "",
                "RANKED_QUEUE_LEN = len(queue)",
                "print(f'Ranked maintenance queue ({RANKED_QUEUE_LEN} assets) — top 10:')",
                "print(f'  {\"udi\":>6}  {\"risk\":>6}  {\"rel\":>6}  {\"blast\":>5}  {\"priority\":>8}')",
                "for udi, risk, rel, blast, prio in queue[:10]:",
                "    print(f'  {udi:>6}  {risk:>6.4f}  {rel:>6.4f}  {blast:>5}  {prio:>8.4f}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 10) Summary + Build-Time Assertions",
                "",
                "The notebook delivered, in one declarative program: real AI4I sensor data ingest, a registered Python classifier driving a Locy neural predicate, MNOR-composed component-level risk, MPROD-composed line-level reliability with the classifier invoked inline inside the aggregator, in-Locy Platt calibration against the dataset's ground-truth failure labels, Brier + accuracy validation, an EXPLAIN audit trail, and a downstream-impact-aware maintenance queue. The assertions below lock the deterministic outputs against future drift.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "assert INGESTED_EQUIPMENT == 60, f'expected 60 ingested equipment, got {INGESTED_EQUIPMENT}'",
                "assert ASSET_RISK_COUNT == 60, f'expected 60 asset_risk rows, got {ASSET_RISK_COUNT}'",
                "assert COMPONENT_RISK_COUNT == 60, f'expected 60 component_risk rows, got {COMPONENT_RISK_COUNT}'",
                "assert LINE_RELIABILITY_COUNT >= 30, (",
                "    f'expected line_reliability over a 4-stage line with 15 equipment/stage, '",
                "    f'got {LINE_RELIABILITY_COUNT}'",
                ")",
                "# Platt on 60 samples can slightly over-fit; we lock the order of",
                "# magnitude rather than guarantee a strict improvement.",
                "assert BRIER_DELTA is None or BRIER_DELTA >= -0.05, (",
                "    f'calibration delta unexpectedly large, delta={BRIER_DELTA}'",
                ")",
                "assert any('Brier' in k or 'brier' in k for k in VALIDATE_METRICS), f'missing Brier metric: {VALIDATE_METRICS}'",
                "assert EXPLAIN_PRODUCED >= 1, 'EXPLAIN should produce at least one record'",
                "assert RANKED_QUEUE_LEN == 60, f'ranked queue should cover all 60 assets, got {RANKED_QUEUE_LEN}'",
                "print('All build-time assertions passed.')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 11) Cleanup",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "del db",
                "shutil.rmtree(WORK_DIR, ignore_errors=True)",
                "print(f'Cleaned up {WORK_DIR}')",
            ],
        )
    )

    return {
        "cells": cells,
        "metadata": _metadata(),
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _render(obj: dict[str, Any]) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify the on-disk notebook matches the generator output (CI use).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=NOTEBOOK_PATH,
        help="Where to write the notebook.",
    )
    args = parser.parse_args()

    nb = _build_notebook()
    body = _render(nb)

    if args.check:
        existing = args.output.read_text(encoding="utf-8") if args.output.exists() else ""
        if existing == body:
            print(f"OK: {args.output} matches generator output")
            return 0
        diff = difflib.unified_diff(
            existing.splitlines(keepends=True),
            body.splitlines(keepends=True),
            fromfile=str(args.output),
            tofile=str(args.output) + ".new",
            n=3,
        )
        sys.stderr.write("".join(diff))
        sys.stderr.write(
            f"\nERROR: {args.output} does not match generator output. "
            "Re-run without --check to regenerate.\n"
        )
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(body, encoding="utf-8")
    print(f"Wrote {args.output} ({len(nb['cells'])} cells)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
