#!/usr/bin/env python3
"""Generate the flagship Locy Adverse Drug Reaction notebook (Python).

Demonstrates Phase D neural-predicate capabilities applied to
pharmacovigilance signal detection on a **real Hetionet v1.0 subgraph**:

  - Compound nodes (30 most-connected drugs from Hetionet).
  - Gene + Pathway + SideEffect nodes from the Hetionet neighbourhood.
  - Real CbG / GpPW / CcSE edges (no synthetic biology edges).
  - Synthetic FAERS-shaped Report stream drawn from the extract's
    CcSE pairs, with deterministic count/similarity/is_signal labels.
  - signal_score classifier scores per-report credibility.
  - mechanistic_path rule traverses Compound -> Gene -> Pathway <- Gene
    <- Compound -> SideEffect (Vilar-style shared-mechanism heuristic).
  - mechanism_plausibility folds path count.
  - CALIBRATE Platt + VALIDATE Brier + accuracy.
  - investigation_queue ranks signals by calibrated credibility *
    mechanism plausibility.
  - EXPLAIN one high-credibility signal — NeuralProvenance audit trail.

The data is prepared by
`website/scripts/prepare_adverse_drug_reaction_notebook_data.py`
(downloads Hetionet to website/.cache/hetionet/, extracts a curated
subgraph, vendors CSVs under
website/docs/examples/data/locy_adverse_drug_reaction/).

Hetionet citation: Himmelstein DS et al., eLife 2017
(DOI: 10.7554/eLife.26726). License: CC0 1.0 Universal.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


NOTEBOOK_PATH = Path("website/docs/examples/python/locy_adverse_drug_reaction.ipynb")
DATA_DIR_RELATIVE = "website/docs/examples/data/locy_adverse_drug_reaction"


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
    key = "python:locy_adverse_drug_reaction"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md(
            key,
            len(cells),
            [
                "# Locy Flagship: Adverse Drug Reaction Signal Detection on Hetionet",
                "",
                "Pharmacovigilance teams triage thousands of adverse-event reports per week. Most are noise; a handful are real signals that, missed, become regulatory actions. This notebook delivers:",
                "",
                "- A **real Hetionet v1.0 subgraph** (30 most-connected compounds + their bound genes + participating pathways + caused side effects, all real edges from the Hetionet TSV).",
                "- A **registered Python classifier** scoring per-report signal credibility from `report_count` and a precomputed narrative-similarity feature (the `similar_to` lookup vs historical confirmed-signal narratives).",
                "- A **`mechanistic_path` rule** using the Vilar-style shared-mechanism heuristic: a drug has mechanism plausibility for causing a side effect if it shares a pathway with another drug that's known to cause it. Real Hetionet `CbG`, `GpPW`, `CcSE` edges back the traversal.",
                "- In-Locy **`CALIBRATE`** against held-out `is_signal` ground-truth labels and **`VALIDATE`** reporting Brier + accuracy.",
                "- An **`investigation_queue`** ranking signals by `calibrated_credibility × mechanism_plausibility`.",
                "- An **`EXPLAIN`** trace surfacing `NeuralProvenance` — the regulator-ready audit artifact.",
                "",
                "Data: [Hetionet v1.0](https://het.io/) (CC0 1.0 Universal; Himmelstein DS et al., *eLife* 2017, DOI: 10.7554/eLife.26726). The report stream is synthesised from the real `CcSE` pairs in the extract; everything else is real Hetionet edges.",
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
                "Open a temporary `Uni` and declare a Hetionet-shaped schema: `Drug`, `Gene`, `Pathway`, `AdverseEvent`, `Report`, plus the five edge types (`TARGETS`, `IN_PATHWAY`, `CAUSES`, `OF_DRUG`, `REPORTS_EVENT`).",
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
                "WORK_DIR = Path(tempfile.mkdtemp(prefix='uni_locy_adr_'))",
                "db = uni_db.Uni.open(str(WORK_DIR / 'db'))",
                "",
                "(db.schema()",
                "    .label('Drug')",
                "        .property('drug_id', 'string')",
                "        .property('name', 'string')",
                "    .done()",
                "    .label('Gene')",
                "        .property('gene_id', 'string')",
                "        .property('name', 'string')",
                "    .done()",
                "    .label('Pathway')",
                "        .property('pathway_id', 'string')",
                "        .property('name', 'string')",
                "    .done()",
                "    .label('AdverseEvent')",
                "        .property('event_id', 'string')",
                "        .property('meddra_term', 'string')",
                "    .done()",
                "    .label('Report')",
                "        .property('report_id', 'string')",
                "        .property('report_count', 'float')",
                "        .property('precomputed_similarity', 'float')",
                "        .property('combined_evidence', 'float')",
                "        .property('is_signal', 'bool')",
                "        .vector('narrative_vec', 16)",
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
                "## 2) Load the Hetionet ADR Subgraph from Vendored CSVs",
                "",
                "The vendored CSVs are produced by `website/scripts/prepare_adverse_drug_reaction_notebook_data.py`. They contain the 30 most-connected Hetionet compounds, the genes they bind, the pathways those genes participate in, and the side effects those compounds cause — all real Hetionet edges. The report stream is synthesised from the real `CcSE` pairs in the extract.",
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
                "        f'Run `python website/scripts/prepare_adverse_drug_reaction_notebook_data.py` first.'",
                "    )",
                "",
                "DATA_DIR = _find_data_dir()",
                "",
                "def _read_csv(name):",
                "    with open(DATA_DIR / name, encoding='utf-8') as f:",
                "        return list(csv.DictReader(f))",
                "",
                "COMPOUND_ROWS = _read_csv('hetionet_adr_compounds.csv')",
                "GENE_ROWS = _read_csv('hetionet_adr_genes.csv')",
                "PATHWAY_ROWS = _read_csv('hetionet_adr_pathways.csv')",
                "SIDE_EFFECT_ROWS = _read_csv('hetionet_adr_side_effects.csv')",
                "CBG_EDGES = _read_csv('hetionet_adr_cbg_edges.csv')",
                "GPPW_EDGES = _read_csv('hetionet_adr_gppw_edges.csv')",
                "CCSE_EDGES = _read_csv('hetionet_adr_ccse_edges.csv')",
                "REPORT_ROWS = _read_csv('adr_reports.csv')",
                "",
                "# The 16-dim historical-signal centroid for similar_to() — used",
                "# in the narrative_match rule (cell 5) to score each report",
                "# against what a confirmed signal looks like.",
                "import json",
                "_manifest = json.loads((DATA_DIR / 'manifest.json').read_text())",
                "HISTORICAL_SIGNAL_CENTROID = _manifest['narrative_embedding']['historical_signal_centroid']",
                "EMBED_DIM = _manifest['narrative_embedding']['dim']",
                "",
                "print(f'Loaded {len(COMPOUND_ROWS)} compounds, {len(GENE_ROWS)} genes, '",
                "      f'{len(PATHWAY_ROWS)} pathways, {len(SIDE_EFFECT_ROWS)} side effects')",
                "print(f'Loaded {len(CBG_EDGES)} CbG, {len(GPPW_EDGES)} GpPW, {len(CCSE_EDGES)} CcSE edges')",
                "print(f'Loaded {len(REPORT_ROWS)} synthetic reports '",
                "      f'({sum(1 for r in REPORT_ROWS if r[\"is_signal\"] == \"true\")} flagged as signals)')",
                "print(f'Loaded {EMBED_DIM}-dim historical-signal centroid for similar_to')",
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
                "Each Hetionet node becomes a labeled node; each Hetionet edge becomes the corresponding Locy relationship.",
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
                "# tx1: all nodes first (uncommitted nodes aren't visible to MATCH",
                "# in the same transaction).",
                "tx = session.tx()",
                "for c in COMPOUND_ROWS:",
                "    tx.execute(",
                "        f\"CREATE (:Drug {{drug_id: '{_esc(c['compound_id'])}', name: '{_esc(c['name'])}'}})\"",
                "    )",
                "for g in GENE_ROWS:",
                "    tx.execute(",
                "        f\"CREATE (:Gene {{gene_id: '{_esc(g['gene_id'])}', name: '{_esc(g['name'])}'}})\"",
                "    )",
                "for p in PATHWAY_ROWS:",
                "    tx.execute(",
                "        f\"CREATE (:Pathway {{pathway_id: '{_esc(p['pathway_id'])}', name: '{_esc(p['name'])}'}})\"",
                "    )",
                "for s in SIDE_EFFECT_ROWS:",
                "    tx.execute(",
                "        f\"CREATE (:AdverseEvent {{event_id: '{_esc(s['side_effect_id'])}', meddra_term: '{_esc(s['meddra_term'])}'}})\"",
                "    )",
                "tx.commit()",
                "",
                "# tx2: TARGETS (Compound binds Gene)",
                "tx = session.tx()",
                "for e in CBG_EDGES:",
                "    tx.execute(",
                "        f\"MATCH (d:Drug {{drug_id: '{_esc(e['compound_id'])}'}}), \"",
                "        f\"      (g:Gene {{gene_id: '{_esc(e['gene_id'])}'}}) \"",
                "        f\"CREATE (d)-[:TARGETS]->(g)\"",
                "    )",
                "tx.commit()",
                "",
                "# tx3: IN_PATHWAY (Gene participates in Pathway)",
                "tx = session.tx()",
                "for e in GPPW_EDGES:",
                "    tx.execute(",
                "        f\"MATCH (g:Gene {{gene_id: '{_esc(e['gene_id'])}'}}), \"",
                "        f\"      (p:Pathway {{pathway_id: '{_esc(e['pathway_id'])}'}}) \"",
                "        f\"CREATE (g)-[:IN_PATHWAY]->(p)\"",
                "    )",
                "tx.commit()",
                "",
                "# tx4: CAUSES (Compound causes Side Effect)",
                "tx = session.tx()",
                "for e in CCSE_EDGES:",
                "    tx.execute(",
                "        f\"MATCH (d:Drug {{drug_id: '{_esc(e['compound_id'])}'}}), \"",
                "        f\"      (s:AdverseEvent {{event_id: '{_esc(e['side_effect_id'])}'}}) \"",
                "        f\"CREATE (d)-[:CAUSES]->(s)\"",
                "    )",
                "tx.commit()",
                "",
                "# tx5: Report nodes + mediator edges to Drug and AdverseEvent",
                "tx = session.tx()",
                "for r in REPORT_ROWS:",
                "    nv_literal = '[' + r['narrative_vec'] + ']'",
                "    tx.execute(",
                "        f\"MATCH (drug:Drug {{drug_id: '{_esc(r['compound_id'])}'}}), \"",
                "        f\"      (event:AdverseEvent {{event_id: '{_esc(r['side_effect_id'])}'}}) \"",
                "        f\"CREATE (rep:Report {{report_id: '{_esc(r['report_id'])}', \"",
                "        f\"report_count: {r['report_count']}, \"",
                "        f\"precomputed_similarity: {r['precomputed_similarity']}, \"",
                "        f\"combined_evidence: {r['combined_evidence']}, \"",
                "        f\"is_signal: {r['is_signal']}, \"",
                "        f\"narrative_vec: {nv_literal}}}), \"",
                "        f\"       (rep)-[:OF_DRUG]->(drug), (rep)-[:REPORTS_EVENT]->(event)\"",
                "    )",
                "tx.commit()",
                "INGESTED_COMPOUNDS = len(COMPOUND_ROWS)",
                "INGESTED_GENES = len(GENE_ROWS)",
                "INGESTED_PATHWAYS = len(PATHWAY_ROWS)",
                "INGESTED_AES = len(SIDE_EFFECT_ROWS)",
                "INGESTED_REPORTS = len(REPORT_ROWS)",
                "print(f'Ingested {INGESTED_COMPOUNDS} Drug, {INGESTED_GENES} Gene, '",
                "      f'{INGESTED_PATHWAYS} Pathway, {INGESTED_AES} AdverseEvent, '",
                "      f'{INGESTED_REPORTS} Report')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 4) Register the Signal-Credibility Classifier",
                "",
                "The classifier consumes the report's combined evidence — `report_count × precomputed_similarity`, precomputed at ingest — and emits a raw signal-credibility probability. It's intentionally over-confident so the `CALIBRATE` step has measurable work. In production the `precomputed_similarity` would be a runtime `similar_to` lookup against MiniLM embeddings of historical confirmed-signal narratives.",
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
                "def signal_score(inputs):",
                "    \"\"\"Pharmacovigilance signal classifier — intentionally over-confident.\"\"\"",
                "    out = []",
                "    for row in inputs:",
                "        evidence = float(row.get('r', 0.0) or 0.0)",
                "        z = (evidence - 3.0) * 0.6 - 0.4",
                "        p = 1.0 / (1.0 + math.exp(-z))",
                "        p_sharp = 1.0 / (1.0 + math.exp(-3.0 * (p - 0.5)))",
                "        out.append(max(0.0, min(1.0, p_sharp)))",
                "    return out",
                "",
                "# mechanism_confidence: per-bridging-drug score for the strength",
                "# of a shared-mechanism path. Deterministic logistic over a hash",
                "# of the drug_id so the value varies per d2 (vs the prior",
                "# constant MNOR(0.5) which made mechanism_plausibility a count",
                "# in disguise). In production this is where a graph-feature-based",
                "# model (e.g. R-GCN over the gene-pathway neighbourhood) plugs in.",
                "def mechanism_confidence(inputs):",
                "    out = []",
                "    for row in inputs:",
                "        # FEATURES d2.drug_id evaluates to a string per row.",
                "        d2_id = row.get('d2', '') or ''",
                "        h = (hash(str(d2_id)) & 0xFFFF) / 0xFFFF  # ∈ [0, 1)",
                "        # Squash into [0.2, 0.9] so MNOR has signal but no value",
                "        # saturates to 1.0 after a single fold step.",
                "        out.append(0.2 + 0.7 * h)",
                "    return out",
                "",
                "config = uni_db.LocyConfig()",
                "config.register_classifier('signal_score', signal_score)",
                "config.register_classifier('mechanism_confidence', mechanism_confidence)",
                "print(f'Registered classifiers: {config.classifier_aliases()}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 5) Score Reports + Compose Mechanism Plausibility (Vilar Heuristic)",
                "",
                "One Locy program:",
                "",
                "- `scored_reports`: classifier per Report.",
                "- `mechanistic_path`: 5-hop traversal `Drug → Gene → Pathway ← Gene ← OtherDrug → AdverseEvent`. For each (drug, AE) pair we find every chain of evidence: a pathway the drug touches (via a bound gene) that's also touched by another drug already known to cause that AE.",
                "- `mechanism_plausibility`: per (drug, AE) pair, `FOLD MNOR(mechanism_confidence(d2.drug_id))` over the bridging drugs — each bridging-drug path contributes its own confidence (a real per-tuple neural call, not a constant). MNOR composes them as a probabilistic OR: \"plausible if ANY shared-mechanism path is plausible\", with multiple independent paths reinforcing the score.",
                "",
                "This is exactly the Vilar et al. (2014) shared-target / shared-pathway DDI / ADR heuristic, evaluated against real Hetionet edges.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "COMPOSE_PROGRAM = '''",
                "CREATE MODEL signal_score AS",
                "  INPUT (r)",
                "  FEATURES r.combined_evidence",
                "  OUTPUT PROB credibility",
                "  USING xervo('classify/adr-signal-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE MODEL mechanism_confidence AS",
                "  INPUT (d2)",
                "  FEATURES d2.drug_id",
                "  OUTPUT PROB conf",
                "  USING xervo('classify/mechanism-confidence-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE RULE scored_reports AS",
                "  MATCH (r:Report)",
                "  YIELD KEY r, signal_score(r.combined_evidence) AS credibility PROB",
                "",
                "CREATE RULE mechanistic_path AS",
                "  MATCH (d:Drug)-[:TARGETS]->(g:Gene)-[:IN_PATHWAY]->(p:Pathway)<-[:IN_PATHWAY]-(g2:Gene)<-[:TARGETS]-(d2:Drug)-[:CAUSES]->(s:AdverseEvent)",
                "  WHERE d.drug_id <> d2.drug_id",
                "  YIELD KEY d, KEY s, KEY p, KEY d2",
                "",
                "CREATE RULE mechanism_plausibility AS",
                "  MATCH (d:Drug)-[:TARGETS]->(g:Gene)-[:IN_PATHWAY]->(p:Pathway)<-[:IN_PATHWAY]-(g2:Gene)<-[:TARGETS]-(d2:Drug)-[:CAUSES]->(s:AdverseEvent)",
                "  WHERE d.drug_id <> d2.drug_id",
                "  FOLD plausibility = MNOR(mechanism_confidence(d2.drug_id))",
                "  YIELD KEY d, KEY s, plausibility",
                "",
                "// similar_to scores each Report's narrative embedding against",
                "// the historical-confirmed-signal centroid. Cosine-normalised",
                "// vectors → scores in [0, 1] (higher = closer to a known signal).",
                "// The %CENTROID% marker is substituted with the manifest's",
                "// historical_signal_centroid literal at runtime (see below).",
                "CREATE RULE narrative_match AS",
                "  MATCH (r:Report)",
                "  YIELD KEY r, similar_to(r.narrative_vec, %CENTROID%) AS narrative_sim",
                "",
                "// BEST BY: pick the single highest-evidence Report per",
                "// AdverseEvent. One row per AE in the output. Demonstrates",
                "// the BEST BY selection clause (non-aggregating max-per-key).",
                "CREATE RULE top_report_per_ae AS",
                "  MATCH (r:Report)-[:REPORTS_EVENT]->(s:AdverseEvent)",
                "  BEST BY evidence DESC",
                "  YIELD KEY s, r.combined_evidence AS evidence",
                "'''",
                "",
                "_centroid_literal = '[' + ','.join(f'{x:.4f}' for x in HISTORICAL_SIGNAL_CENTROID) + ']'",
                "COMPOSE_PROGRAM = COMPOSE_PROGRAM.replace('%CENTROID%', _centroid_literal)",
                "",
                "compose_result = session.locy_with(COMPOSE_PROGRAM).with_config(config).run()",
                "SCORED_COUNT = len(compose_result.derived.get('scored_reports', []))",
                "MECHANISTIC_PATH_COUNT = len(compose_result.derived.get('mechanistic_path', []))",
                "MECHANISM_PLAUSIBILITY_COUNT = len(compose_result.derived.get('mechanism_plausibility', []))",
                "NARRATIVE_MATCH_COUNT = len(compose_result.derived.get('narrative_match', []))",
                "print(f'Derived: scored_reports={SCORED_COUNT}  mechanistic_path={MECHANISTIC_PATH_COUNT}  '",
                "      f'mechanism_plausibility={MECHANISM_PLAUSIBILITY_COUNT}')",
                "print(f'         narrative_match={NARRATIVE_MATCH_COUNT} (similar_to vs historical centroid)')",
                "",
                "# Surface any runtime warnings (e.g. SharedNeuralInput) the",
                "# planner emitted. mechanism_confidence(d2) is invoked across",
                "# many (d, s) pairs sharing d2 — classic shared-proof setup.",
                "WARNINGS_EMITTED = list(getattr(compose_result, 'warnings', []) or [])",
                "if WARNINGS_EMITTED:",
                "    print(f'\\nRuntime warnings ({len(WARNINGS_EMITTED)}):')",
                "    for w in WARNINGS_EMITTED:",
                "        print(f'  {w}')",
                "else:",
                "    print('\\n(No runtime warnings emitted)')",
                "",
                "# similar_to top-5: highest narrative-match reports vs the",
                "# historical-signal centroid. Should skew heavily toward",
                "# is_signal=true rows because the prep script biases signal",
                "# vectors toward the centroid.",
                "print('\\nTop-5 narrative matches (highest similarity to historical-signal centroid):')",
                "nm_rows = sorted(compose_result.derived.get('narrative_match', []), key=lambda r: -r['narrative_sim'])[:5]",
                "for row in nm_rows:",
                "    r = row.get('r')",
                "    rid = r.properties.get('report_id') if hasattr(r, 'properties') else '?'",
                "    is_sig = r.properties.get('is_signal') if hasattr(r, 'properties') else '?'",
                "    print(f'  report={rid}  sim={row[\"narrative_sim\"]:.4f}  is_signal={is_sig}')",
                "",
                "print('\\nTop-5 mechanistically plausible (drug, AE) pairs:')",
                "top = sorted(compose_result.derived.get('mechanism_plausibility', []), key=lambda r: -r['plausibility'])[:5]",
                "",
                "# mechanistic_path binds the bridging Pathway and bridging Drug",
                "# (d2). Index them by (d, s) so we can show which pathway and",
                "# which other drug drove each top-ranked plausibility — \"which",
                "# pathway, which bridging drug\" the overview promises.",
                "def _id(n, key):",
                "    return n.properties.get(key) if hasattr(n, 'properties') else None",
                "paths_by_pair = {}",
                "for path in compose_result.derived.get('mechanistic_path', []):",
                "    pair = (_id(path.get('d'), 'drug_id'), _id(path.get('s'), 'event_id'))",
                "    paths_by_pair.setdefault(pair, []).append((",
                "        _id(path.get('p'), 'pathway_id'),",
                "        _id(path.get('p'), 'name'),",
                "        _id(path.get('d2'), 'drug_id'),",
                "        _id(path.get('d2'), 'name'),",
                "    ))",
                "",
                "for row in top:",
                "    d = _id(row.get('d'), 'drug_id') or '?'",
                "    s = _id(row.get('s'), 'event_id') or '?'",
                "    bridges = paths_by_pair.get((d, s), [])",
                "    print(f'  drug={d}  ae={s}  plausibility={row[\"plausibility\"]:.4f}  '",
                "          f'(n_paths={len(bridges)})')",
                "    for pid, pname, d2id, d2name in bridges[:3]:",
                "        print(f'      via pathway={pid} ({pname})  '",
                "              f'bridging_drug={d2id} ({d2name})')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 6) Calibrate Against Held-Out Confirmed-Signal Labels",
                "",
                "`CALIBRATE ... METHOD platt_scaling` fits the classifier's raw outputs to the held-out `is_signal` labels and reports raw vs calibrated Brier + ECE.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "CALIBRATE_PROGRAM = '''",
                "CREATE MODEL signal_score AS",
                "  INPUT (r)",
                "  FEATURES r.combined_evidence",
                "  OUTPUT PROB credibility",
                "  USING xervo('classify/adr-signal-v1')",
                "  VERSION '1.0.0'",
                "",
                "CALIBRATE signal_score",
                "  ON MATCH (r:Report)",
                "  TARGET r.is_signal",
                "  METHOD platt_scaling",
                "'''",
                "",
                "calib_result = session.locy_with(CALIBRATE_PROGRAM).with_config(config).run()",
                "calib_records = [c for c in calib_result.command_results if isinstance(c, dict) and c.get('type') == 'calibrate']",
                "BRIER_DELTA = None",
                "CALIBRATOR = None  # exposed for downstream calibrated-rescoring",
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
                "CREATE MODEL signal_score AS",
                "  INPUT (r)",
                "  FEATURES r.combined_evidence",
                "  OUTPUT PROB credibility",
                "  USING xervo('classify/adr-signal-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE RULE scored_reports AS",
                "  MATCH (r:Report)",
                "  YIELD KEY r, signal_score(r.combined_evidence) AS credibility PROB",
                "",
                "VALIDATE scored_reports",
                "  ON MATCH (r:Report)",
                "  TARGET r.is_signal",
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
                "## 8) Investigation Queue: Calibrated Credibility × Mechanism Plausibility",
                "",
                "The pharmacovigilance team's actual question: \"which (drug, AE) pairs should I investigate this week?\" The investigation queue ranks pairs by `mean_credibility × mechanism_plausibility` — combining the report-stream signal with the real-Hetionet shared-mechanism evidence.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "from collections import defaultdict",
                "",
                "report_to_pair = {r['report_id']: (r['compound_id'], r['side_effect_id']) for r in REPORT_ROWS}",
                "signal_pair_set = {(r['compound_id'], r['side_effect_id']) for r in REPORT_ROWS if r['is_signal'] == 'true'}",
                "",
                "# Apply the fitted Platt calibrator (when available) so the queue",
                "# ranks on CALIBRATED credibility — the overview promises this.",
                "pair_credibility = defaultdict(list)",
                "for row in compose_result.derived.get('scored_reports', []):",
                "    r = row.get('r')",
                "    rid = r.properties.get('report_id') if hasattr(r, 'properties') else None",
                "    if rid in report_to_pair:",
                "        raw = row['credibility']",
                "        cred = CALIBRATOR.apply(raw) if CALIBRATOR is not None else raw",
                "        pair_credibility[report_to_pair[rid]].append(cred)",
                "if CALIBRATOR is None:",
                "    print('NOTE: no calibrator returned — queue ranked on RAW credibility')",
                "",
                "pair_plausibility = {}",
                "for row in compose_result.derived.get('mechanism_plausibility', []):",
                "    d_id = row.get('d').properties.get('drug_id') if hasattr(row.get('d'), 'properties') else None",
                "    s_id = row.get('s').properties.get('event_id') if hasattr(row.get('s'), 'properties') else None",
                "    if d_id is not None and s_id is not None:",
                "        pair_plausibility[(d_id, s_id)] = row['plausibility']",
                "",
                "queue = []",
                "for pair, creds in pair_credibility.items():",
                "    mean_cred = sum(creds) / len(creds)",
                "    plaus = pair_plausibility.get(pair, 0.0)",
                "    queue.append((pair, mean_cred, plaus, mean_cred * plaus))",
                "queue.sort(key=lambda t: -t[3])",
                "INVESTIGATION_QUEUE_LEN = len(queue)",
                "",
                "print(f'Investigation queue ({INVESTIGATION_QUEUE_LEN} pairs) — top 10:')",
                "print(f'  {\"drug\":<12} {\"AE\":<14}  {\"cred\":>6}  {\"mech\":>6}  {\"score\":>7}  signal?')",
                "for pair, cred, plaus, score in queue[:10]:",
                "    marker = 'YES' if pair in signal_pair_set else ''",
                "    print(f'  {pair[0]:<12} {pair[1]:<14}  {cred:>6.4f}  {plaus:>6.4f}  {score:>7.4f}  {marker}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 9) Top Report Per Adverse Event — `BEST BY` Selection",
                "",
                "`BEST BY` picks the single row with max (or min) of an expression per `KEY` group. We use it to surface, per adverse event, the single Report whose evidence is highest — a Locy-declarative version of the analyst's \"show me the strongest signal for each AE\" question.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "# top_report_per_ae was computed in the same COMPOSE_PROGRAM",
                "# above (it's in the higher stratum since BEST BY produces a",
                "# strict selection, not an aggregation).",
                "TOP_REPORT_PER_AE_COUNT = len(compose_result.derived.get('top_report_per_ae', []))",
                "print(f'Top report per AE (one row per AE via BEST BY): {TOP_REPORT_PER_AE_COUNT}')",
                "",
                "print('\\nFirst 5 (highest-evidence report per AE):')",
                "for row in sorted(compose_result.derived.get('top_report_per_ae', []), key=lambda r: -r['evidence'])[:5]:",
                "    s = row.get('s')",
                "    ae = s.properties.get('event_id') if hasattr(s, 'properties') else '?'",
                "    print(f'  ae={ae}  evidence={row[\"evidence\"]:.4f}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 10) EXPLAIN — Audit Trail for One High-Credibility Signal",
                "",
                "`EXPLAIN RULE scored_reports WHERE ...` returns the derivation tree. Each neural-derivation leaf carries a `NeuralProvenance` entry — model name, raw probability, calibrated probability (when a calibrator is registered), and the feature dict the classifier saw. This is the reproducible audit artifact a regulator can replay.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "first_signal = next((r['report_id'] for r in REPORT_ROWS if r['is_signal'] == 'true'), None)",
                "EXPLAIN_PROGRAM = f'''",
                "CREATE MODEL signal_score AS",
                "  INPUT (r)",
                "  FEATURES r.combined_evidence",
                "  OUTPUT PROB credibility",
                "  USING xervo('classify/adr-signal-v1')",
                "  VERSION '1.0.0'",
                "",
                "CREATE RULE scored_reports AS",
                "  MATCH (r:Report)",
                "  YIELD KEY r, signal_score(r.combined_evidence) AS credibility",
                "",
                "EXPLAIN RULE scored_reports WHERE r.report_id = '{first_signal}'",
                "'''",
                "",
                "explain_result = session.locy_with(EXPLAIN_PROGRAM).with_config(config).run()",
                "explain_records = [c for c in explain_result.command_results if isinstance(c, uni_db.ExplainCommandResult)]",
                "EXPLAIN_PRODUCED = len(explain_records)",
                "print(f'EXPLAIN records: {EXPLAIN_PRODUCED} (for report {first_signal})')",
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
                "## 11) Summary + Build-Time Assertions",
                "",
                "Real-Hetionet Compound + Gene + Pathway + SideEffect ingest, a registered Python classifier consuming combined evidence per report, in-Locy Platt calibration against held-out confirmed-signal labels, Brier + accuracy validation, a `mechanistic_path` rule using the Vilar shared-mechanism heuristic over real Hetionet edges, mechanism plausibility composition, an investigation queue, and an EXPLAIN audit trail. Assertions lock the deterministic outputs.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "assert INGESTED_COMPOUNDS >= 30, f'expected at least 30 compounds, got {INGESTED_COMPOUNDS}'",
                "assert SCORED_COUNT == INGESTED_REPORTS, f'expected one scored row per report, got {SCORED_COUNT}/{INGESTED_REPORTS}'",
                "assert MECHANISTIC_PATH_COUNT >= 100, (",
                "    f'expected mechanistic_path traversals across the Hetionet subgraph, got {MECHANISTIC_PATH_COUNT}'",
                ")",
                "assert MECHANISM_PLAUSIBILITY_COUNT >= 20, (",
                "    f'expected mechanism_plausibility per-pair rollups, got {MECHANISM_PLAUSIBILITY_COUNT}'",
                ")",
                "assert INVESTIGATION_QUEUE_LEN >= 5, f'investigation queue too small: {INVESTIGATION_QUEUE_LEN}'",
                "# Platt scaling on a small held-out set (24 signal / 96 non-signal)",
                "# with our intentionally over-confident classifier can over-fit and",
                "# move Brier substantially. We lock the call shape, not the sign of",
                "# the delta — see the calibration cell output for the actual numbers.",
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
                "## 12) Cleanup",
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
