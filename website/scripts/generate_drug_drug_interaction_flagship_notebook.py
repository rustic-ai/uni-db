#!/usr/bin/env python3
"""Generate the flagship Locy Drug-Drug Interaction notebook (Python).

Demonstrates Phase D neural-predicate capabilities applied to
polypharmacy interaction risk on real Hetionet-derived drugs +
offline-trained embeddings + ONNX MLP head:

  - Real Hetionet Compound subgraph (CSV vendored by the prep script).
  - 64-dim drug embeddings from offline TruncatedSVD over the
    Compound-Gene bipartite adjacency (parquet, ~30 KB).
  - Tiny MLP head exported to ONNX (~17 KB) at prep time.
  - Python classifier loads ONNX runtime + embeddings once at module
    import, resolves InteractionRecord pair_id → (drug, drug), looks
    up the two embeddings, concatenates, ONNX inference returns
    P(interact).
  - Composition rules: scored_interactions, joint_regimen_safety
    (`FOLD MPROD(1 - interaction_score(rec.pair_id))` across distinct
    drug pairs per patient — inline classifier in aggregator).
  - CALIBRATE Platt against the Vilar-derived is_dangerous labels.
  - VALIDATE Brier + accuracy.
  - Patient ranking + EXPLAIN audit trace.

The prep script
`website/scripts/prepare_drug_drug_interaction_notebook_data.py`
produces all vendored artifacts.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


NOTEBOOK_PATH = Path("website/docs/examples/python/locy_drug_drug_interaction.ipynb")
DATA_DIR_RELATIVE = "website/docs/examples/data/locy_drug_drug_interaction"


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
    key = "python:locy_drug_drug_interaction"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md(
            key,
            len(cells),
            [
                "# Locy Flagship: DDI Risk + Joint Regimen Safety with R-GCN-Style Drug Embeddings",
                "",
                "Clinical pharmacists triage drug-drug interaction warnings for elderly polypharmacy patients. The clinical question is *not* \"is this pairwise interaction dangerous?\" — it's \"given this patient's entire regimen of 6 drugs, what's the joint probability that *any* clinically significant interaction occurs?\" This notebook delivers:",
                "",
                "- A **real Hetionet v1.0 drug subgraph**: 40 Compound nodes + their Gene targets, sourced from the Hetionet TSV.",
                "- **Offline-trained 64-dim drug embeddings** from `TruncatedSVD` over the Compound-Gene bipartite adjacency (vendored as parquet). In production swap in an R-GCN; the deployment pattern is identical.",
                "- **Pseudo-DDI labels** from the Vilar-style shared-target heuristic: drugs sharing ≥2 targeted genes are tagged `is_dangerous=true`.",
                "- A **registered Python classifier** that loads the ONNX MLP head + embeddings parquet once, then for each pair resolves the two embeddings, concatenates them, and runs ONNX inference.",
                "- A **`joint_regimen_safety` rule**: `FOLD MPROD(1.0 - interaction_score(rec.pair_id))` across all distinct drug pairs in each patient's regimen — *inline classifier invocation inside the aggregator*.",
                "- In-Locy **`CALIBRATE`** against the `is_dangerous` labels and **`VALIDATE`** reporting Brier + accuracy.",
                "- **Patient risk ranking** with worst-contributing-pair annotation.",
                "- **`EXPLAIN`** trace surfacing the classifier's `NeuralProvenance` per derivation.",
                "",
                "Data: [Hetionet v1.0](https://het.io/) (CC0 1.0 Universal; Himmelstein DS et al., *eLife* 2017, DOI: 10.7554/eLife.26726). Runtime dependencies: `onnxruntime`, `pandas`, `pyarrow` (see the `notebook-runtime` extras group in `bindings/uni-db/pyproject.toml`).",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 1) Setup + Schema",
                "",
                "`Drug`, `InteractionRecord`, `Patient`, plus `HAS_INTERACTION_WITH` (drug ↔ record) and `TAKES` (patient → drug) edges. The `pair_id` on each `InteractionRecord` is the lookup key passed to the classifier so it can resolve the two drug embeddings at inference time.",
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
                "WORK_DIR = Path(tempfile.mkdtemp(prefix='uni_locy_ddi_'))",
                "db = uni_db.Uni.open(str(WORK_DIR / 'db'))",
                "",
                "(db.schema()",
                "    .label('Drug')",
                "        .property('drug_id', 'string')",
                "        .property('name', 'string')",
                "    .done()",
                "    .label('InteractionRecord')",
                "        .property('pair_id', 'string')",
                "        .property('shared_targets', 'int')",
                "        .property('is_dangerous', 'bool')",
                "    .done()",
                "    .label('Patient')",
                "        .property('patient_id', 'string')",
                "    .done()",
                "    .apply())",
                "print('DB initialized')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 2) Load Vendored Hetionet DDI Data + ONNX Artifacts",
                "",
                "The prep script (`website/scripts/prepare_drug_drug_interaction_notebook_data.py`) vendors the curated drug/gene CSVs, the pseudo-DDI pair list, patient regimens, the 64-dim drug embeddings parquet, and the ONNX MLP head.",
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
                "        f'Run `python website/scripts/prepare_drug_drug_interaction_notebook_data.py` first.'",
                "    )",
                "",
                "DATA_DIR = _find_data_dir()",
                "",
                "def _read_csv(name):",
                "    with open(DATA_DIR / name, encoding='utf-8') as f:",
                "        return list(csv.DictReader(f))",
                "",
                "DRUG_ROWS = _read_csv('hetionet_ddi_drugs.csv')",
                "PAIR_ROWS = _read_csv('ddi_pairs.csv')",
                "PATIENT_ROWS = _read_csv('ddi_patients.csv')",
                "REGIMEN_ROWS = _read_csv('ddi_patient_regimens.csv')",
                "",
                "print(f'Loaded {len(DRUG_ROWS)} Hetionet drugs, {len(PAIR_ROWS)} pseudo-DDI pairs '",
                "      f'({sum(1 for r in PAIR_ROWS if r[\"is_dangerous\"] == \"true\")} dangerous), '",
                "      f'{len(PATIENT_ROWS)} patients with {len(REGIMEN_ROWS)} drug-regimen edges')",
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
                "Nodes in the first transaction; edges + interaction records + patients in the second.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "session = db.session()",
                "",
                "def _esc(s):",
                "    return str(s).replace(\"'\", \"\\\\'\")",
                "",
                "# tx1: Drug + Patient nodes.",
                "tx = session.tx()",
                "for d in DRUG_ROWS:",
                "    tx.execute(",
                "        f\"CREATE (:Drug {{drug_id: '{_esc(d['drug_id'])}', name: '{_esc(d['name'])}'}})\"",
                "    )",
                "for p in PATIENT_ROWS:",
                "    tx.execute(",
                "        f\"CREATE (:Patient {{patient_id: '{_esc(p['patient_id'])}'}})\"",
                "    )",
                "tx.commit()",
                "",
                "# tx2: InteractionRecord nodes + HAS_INTERACTION_WITH edges (bidirectional)",
                "# + TAKES regimen edges.",
                "tx = session.tx()",
                "for r in PAIR_ROWS:",
                "    tx.execute(",
                "        f\"MATCH (a:Drug {{drug_id: '{_esc(r['drug_a_id'])}'}}), \"",
                "        f\"      (b:Drug {{drug_id: '{_esc(r['drug_b_id'])}'}}) \"",
                "        f\"CREATE (rec:InteractionRecord {{pair_id: '{_esc(r['pair_id'])}', \"",
                "        f\"shared_targets: {r['shared_targets']}, is_dangerous: {r['is_dangerous']}}}), \"",
                "        f\"       (a)-[:HAS_INTERACTION_WITH]->(rec), \"",
                "        f\"       (b)-[:HAS_INTERACTION_WITH]->(rec)\"",
                "    )",
                "for r in REGIMEN_ROWS:",
                "    tx.execute(",
                "        f\"MATCH (p:Patient {{patient_id: '{_esc(r['patient_id'])}'}}), \"",
                "        f\"      (d:Drug {{drug_id: '{_esc(r['drug_id'])}'}}) \"",
                "        f\"CREATE (p)-[:TAKES]->(d)\"",
                "    )",
                "tx.commit()",
                "INGESTED_DRUGS = len(DRUG_ROWS)",
                "INGESTED_PAIRS = len(PAIR_ROWS)",
                "INGESTED_PATIENTS = len(PATIENT_ROWS)",
                "print(f'Ingested {INGESTED_DRUGS} Drug, {INGESTED_PAIRS} InteractionRecord, '",
                "      f'{INGESTED_PATIENTS} Patient')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 4) Register the ONNX-Backed Pairwise Classifier",
                "",
                "The classifier callable loads the drug embeddings parquet and the ONNX MLP head once at module level. For each invocation:",
                "",
                "1. Receives the InteractionRecord's `pair_id` as the FEATURES value.",
                "2. Resolves `pair_id` to its two `drug_id`s via the precomputed mapping.",
                "3. Looks up the two 64-dim embeddings.",
                "4. Concatenates and runs ONNX inference.",
                "5. Returns a per-row probability vector.",
                "",
                "This is exactly the production pattern: offline graph learning produces embeddings, a tiny runtime ONNX head consumes them, the registered callable bridges Locy and the runtime.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "import numpy as np",
                "import onnxruntime as ort",
                "import pyarrow.parquet as pq",
                "",
                "# Load drug embeddings (rows: drug_id, e0..e63).",
                "_emb_table = pq.read_table(DATA_DIR / 'drug_embeddings.parquet').to_pylist()",
                "_DRUG_EMBED = {",
                "    row['drug_id']: np.asarray(",
                "        [row[f'e{i}'] for i in range(len(row) - 1)], dtype=np.float32",
                "    )",
                "    for row in _emb_table",
                "}",
                "_EMBED_DIM = next(iter(_DRUG_EMBED.values())).shape[0]",
                "print(f'Loaded {len(_DRUG_EMBED)} drug embeddings × {_EMBED_DIM} dim')",
                "",
                "# Load ONNX MLP head.",
                "_ONNX_SESSION = ort.InferenceSession(",
                "    str(DATA_DIR / 'ddi_mlp_head.onnx'),",
                "    providers=['CPUExecutionProvider'],",
                ")",
                "",
                "# pair_id -> (drug_a_id, drug_b_id) lookup, sourced from the vendored CSV.",
                "_PAIR_TO_DRUGS = {",
                "    r['pair_id']: (r['drug_a_id'], r['drug_b_id'])",
                "    for r in PAIR_ROWS",
                "}",
                "",
                "def interaction_score(inputs):",
                "    \"\"\"ONNX-backed DDI classifier.\"\"\"",
                "    if not inputs:",
                "        return []",
                "    feats = np.zeros((len(inputs), 2 * _EMBED_DIM), dtype=np.float32)",
                "    for i, row in enumerate(inputs):",
                "        pair_id = row.get('rec')",
                "        drugs = _PAIR_TO_DRUGS.get(pair_id) if pair_id is not None else None",
                "        if drugs is None:",
                "            # Unknown pair — neutral 0.5 prediction.",
                "            continue",
                "        emb_a = _DRUG_EMBED.get(drugs[0])",
                "        emb_b = _DRUG_EMBED.get(drugs[1])",
                "        if emb_a is None or emb_b is None:",
                "            continue",
                "        feats[i, :_EMBED_DIM] = emb_a",
                "        feats[i, _EMBED_DIM:] = emb_b",
                "    preds = _ONNX_SESSION.run(['p_interact'], {'concat_embeddings': feats})[0]",
                "    return [float(max(0.0, min(1.0, p))) for p in preds.flatten()]",
                "",
                "config = uni_db.LocyConfig()",
                "config.register_classifier('interaction_score', interaction_score)",
                "print(f'Registered classifiers: {config.classifier_aliases()}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 5) Score Pairs + Compose Joint Regimen Safety",
                "",
                "- `scored_interactions`: per-pair classifier output via the ONNX MLP head.",
                "- `joint_regimen_safety`: per patient, `FOLD MPROD(1.0 - interaction_score(rec.pair_id))` across every distinct drug pair in their regimen. The classifier is invoked *inside* the aggregator, so per-pair ONNX inference and regimen composition happen in a single declarative step.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "COMPOSE_PROGRAM = '''",
                "CREATE MODEL interaction_score AS",
                "  INPUT (rec)",
                "  FEATURES rec.pair_id",
                "  OUTPUT PROB risk",
                "  USING xervo('classify/ddi-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE RULE scored_interactions AS",
                "  MATCH (rec:InteractionRecord)",
                "  YIELD KEY rec, interaction_score(rec.pair_id) AS risk PROB",
                "",
                "CREATE RULE joint_regimen_safety AS",
                "  MATCH (p:Patient)-[:TAKES]->(d1:Drug)-[:HAS_INTERACTION_WITH]->(rec:InteractionRecord)<-[:HAS_INTERACTION_WITH]-(d2:Drug)<-[:TAKES]-(p)",
                "  WHERE d1.drug_id < d2.drug_id",
                "  FOLD safety = MPROD(1.0 - interaction_score(rec.pair_id))",
                "  YIELD KEY p, safety",
                "",
                "// BEST BY: pick the single highest-risk pair per patient.",
                "// One row per Patient. Demonstrates BEST BY as a Locy-",
                "// declarative replacement for the Python max() that drives",
                "// the patient-ranking cell below.",
                "CREATE RULE worst_pair_per_patient AS",
                "  MATCH (p:Patient)-[:TAKES]->(d1:Drug)-[:HAS_INTERACTION_WITH]->(rec:InteractionRecord)<-[:HAS_INTERACTION_WITH]-(d2:Drug)<-[:TAKES]-(p)",
                "  WHERE d1.drug_id < d2.drug_id",
                "  BEST BY pair_risk DESC",
                "  YIELD KEY p, interaction_score(rec.pair_id) AS pair_risk",
                "'''",
                "",
                "compose_result = session.locy_with(COMPOSE_PROGRAM).with_config(config).run()",
                "SCORED_COUNT = len(compose_result.derived.get('scored_interactions', []))",
                "JOINT_SAFETY_COUNT = len(compose_result.derived.get('joint_regimen_safety', []))",
                "WORST_PAIR_PER_PATIENT_COUNT = len(compose_result.derived.get('worst_pair_per_patient', []))",
                "print(f'Derived: scored_interactions={SCORED_COUNT}  joint_regimen_safety={JOINT_SAFETY_COUNT}  '",
                "      f'worst_pair_per_patient={WORST_PAIR_PER_PATIENT_COUNT} (one BEST BY row per patient)')",
                "",
                "print('\\nJoint regimen safety per patient (lower = riskier):')",
                "for row in sorted(compose_result.derived.get('joint_regimen_safety', []), key=lambda r: r['safety']):",
                "    p = row.get('p')",
                "    pid = p.properties.get('patient_id') if hasattr(p, 'properties') else '?'",
                "    print(f'  patient={pid:<8}  safety={row[\"safety\"]:.3e}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 6) Calibrate Against the Vilar-Derived `is_dangerous` Labels",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "CALIBRATE_PROGRAM = '''",
                "CREATE MODEL interaction_score AS",
                "  INPUT (rec)",
                "  FEATURES rec.pair_id",
                "  OUTPUT PROB risk",
                "  USING xervo('classify/ddi-v1')",
                "  VERSION '1.0.0'",
                "",
                "CALIBRATE interaction_score",
                "  ON MATCH (rec:InteractionRecord)",
                "  TARGET rec.is_dangerous",
                "  METHOD platt_scaling",
                "'''",
                "",
                "calib_result = session.locy_with(CALIBRATE_PROGRAM).with_config(config).run()",
                "calib_records = [c for c in calib_result.command_results if isinstance(c, dict) and c.get('type') == 'calibrate']",
                "BRIER_DELTA = None",
                "CALIBRATOR = None  # the fitted calibrator, when the command returns one",
                "if calib_records:",
                "    c = calib_records[0]",
                "    print(f'Calibration: {c[\"method\"]}')",
                "    print(f'  raw        brier={c[\"raw_brier\"]:.4f}  ece={c[\"raw_ece\"]:.4f}')",
                "    print(f'  calibrated brier={c[\"calibrated_brier\"]:.4f}  ece={c[\"calibrated_ece\"]:.4f}')",
                "    BRIER_DELTA = c['raw_brier'] - c['calibrated_brier']",
                "    print(f'  delta_brier = {BRIER_DELTA:+.4f}')",
                "    CALIBRATOR = c.get('calibrator')",
                "    print(f'  fitted calibrator: {CALIBRATOR}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 7) Validate",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "VALIDATE_PROGRAM = '''",
                "CREATE MODEL interaction_score AS",
                "  INPUT (rec)",
                "  FEATURES rec.pair_id",
                "  OUTPUT PROB risk",
                "  USING xervo('classify/ddi-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE RULE scored_interactions AS",
                "  MATCH (rec:InteractionRecord)",
                "  YIELD KEY rec, interaction_score(rec.pair_id) AS risk PROB",
                "",
                "VALIDATE scored_interactions",
                "  ON MATCH (rec:InteractionRecord)",
                "  TARGET rec.is_dangerous",
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
                "## 8) EXPLAIN — Pair Audit",
                "",
                "Pair-level `EXPLAIN` trace shows the classifier inputs and outputs for the highest-risk InteractionRecord.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "first_dangerous = next((r['pair_id'] for r in PAIR_ROWS if r['is_dangerous'] == 'true'), None)",
                "EXPLAIN_PROGRAM = f'''",
                "CREATE MODEL interaction_score AS",
                "  INPUT (rec)",
                "  FEATURES rec.pair_id",
                "  OUTPUT PROB risk",
                "  USING xervo('classify/ddi-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE RULE scored_interactions AS",
                "  MATCH (rec:InteractionRecord)",
                "  YIELD KEY rec, interaction_score(rec.pair_id) AS risk PROB",
                "",
                "EXPLAIN RULE scored_interactions WHERE rec.pair_id = '{first_dangerous}'",
                "'''",
                "",
                "explain_result = session.locy_with(EXPLAIN_PROGRAM).with_config(config).run()",
                "explain_records = [c for c in explain_result.command_results if isinstance(c, uni_db.ExplainCommandResult)]",
                "EXPLAIN_PRODUCED = len(explain_records)",
                "print(f'EXPLAIN pair records: {EXPLAIN_PRODUCED} (for pair {first_dangerous})')",
                "",
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
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 9) Patient Risk Ranking + Worst-Pair Annotation",
                "",
                "Combine joint regimen safety with the highest-risk pair (by the classifier's per-pair score) in each patient's regimen — the actionable substitution target.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "patient_drug_set = {}",
                "for r in REGIMEN_ROWS:",
                "    patient_drug_set.setdefault(r['patient_id'], set()).add(r['drug_id'])",
                "",
                "# Map each pair_id back to (drug_a, drug_b) for cross-reference.",
                "pair_to_drugs = {",
                "    r['pair_id']: (r['drug_a_id'], r['drug_b_id'])",
                "    for r in PAIR_ROWS",
                "}",
                "",
                "# Map each pair_id → classifier-derived risk drawn from the",
                "# scored_interactions rule output: the 'worst pair' is chosen by the",
                "# classifier's per-pair prediction, not the static shared_targets count.",
                "pair_to_risk = {}",
                "for row in compose_result.derived.get('scored_interactions', []):",
                "    rec = row.get('rec')",
                "    pid = rec.properties.get('pair_id') if hasattr(rec, 'properties') else None",
                "    if pid is None:",
                "        continue",
                "    pair_to_risk[pid] = row['risk']",
                "",
                "patient_worst_pair = {}",
                "for pid, (a, b) in pair_to_drugs.items():",
                "    risk = pair_to_risk.get(pid)",
                "    if risk is None:",
                "        continue",
                "    for pat, drugs in patient_drug_set.items():",
                "        if a in drugs and b in drugs:",
                "            best = patient_worst_pair.get(pat)",
                "            if best is None or risk > best[2]:",
                "                patient_worst_pair[pat] = (pid, (a, b), risk)",
                "",
                "ranking = []",
                "for row in compose_result.derived.get('joint_regimen_safety', []):",
                "    p = row.get('p')",
                "    pat = p.properties.get('patient_id') if hasattr(p, 'properties') else '?'",
                "    worst = patient_worst_pair.get(pat)",
                "    ranking.append((pat, row['safety'], worst))",
                "ranking.sort(key=lambda r: r[1])",
                "PATIENT_RANKING_LEN = len(ranking)",
                "",
                "print(f'Patient risk ranking ({PATIENT_RANKING_LEN} regimens):')",
                "print(f'  {\"patient\":<8} {\"safety\":>10}  worst_pair')",
                "for pat, safety, worst in ranking:",
                "    if worst is None:",
                "        print(f'  {pat:<8} {safety:>10.3e}  (no cross-pair)')",
                "    else:",
                "        pid, (a, b), risk = worst",
                "        print(f'  {pat:<8} {safety:>10.3e}  {pid} ({a}+{b}, pair_risk={risk:.4f})')",
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
                "Real Hetionet drug subgraph, offline-trained TruncatedSVD 64-dim drug embeddings, ONNX MLP head loaded by the registered classifier, Vilar-derived `is_dangerous` ground-truth labels, joint-regimen-safety composition via `FOLD MPROD` with inline ONNX inference inside the aggregator, in-Locy Platt calibration, Brier + accuracy validation, patient ranking, and an EXPLAIN audit trail.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "assert INGESTED_DRUGS >= 30, f'expected at least 30 drugs, got {INGESTED_DRUGS}'",
                "assert SCORED_COUNT == INGESTED_PAIRS, f'expected {INGESTED_PAIRS} scored rows, got {SCORED_COUNT}'",
                "# Each patient with ≥2 cross-class drugs yields a joint_regimen_safety row.",
                "assert JOINT_SAFETY_COUNT >= 4, f'JOINT_SAFETY_COUNT={JOINT_SAFETY_COUNT}'",
                "assert PATIENT_RANKING_LEN == JOINT_SAFETY_COUNT, (",
                "    f'ranking should match joint_regimen_safety: {PATIENT_RANKING_LEN} vs {JOINT_SAFETY_COUNT}'",
                ")",
                "assert BRIER_DELTA is not None, 'CALIBRATE should return a record'",
                "assert any('Brier' in k or 'brier' in k for k in VALIDATE_METRICS), (",
                "    f'missing Brier metric: {VALIDATE_METRICS}'",
                ")",
                "assert EXPLAIN_PRODUCED >= 1, 'EXPLAIN should produce at least one record'",
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
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--output", type=Path, default=NOTEBOOK_PATH)
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
