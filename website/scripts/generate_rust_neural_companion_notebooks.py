#!/usr/bin/env python3
"""Generate the three Rust companion notebooks for the Phase D neural flagships.

Each companion mirrors the Python flagship's domain at a compact ~12-cell
shape: schema → seed inline → register a MockClassifier → CREATE MODEL +
invocation → CALIBRATE → VALIDATE → EXPLAIN. The Rust side uses
MockClassifier (uni-locy crate) as the registered classifier so the
notebook stays self-contained without ONNX or sklearn.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


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


def _rust_metadata() -> dict[str, Any]:
    return {
        "kernelspec": {
            "display_name": "Rust",
            "language": "rust",
            "name": "rust",
        },
        "language_info": {
            "codemirror_mode": "rust",
            "file_extension": ".rs",
            "mimetype": "text/rust",
            "name": "Rust",
            "pygments_lexer": "rust",
            "version": "",
        },
    }


def _wrap(cells: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "cells": cells,
        "metadata": _rust_metadata(),
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _pdm_notebook() -> dict[str, Any]:
    key = "rust:locy_predictive_maintenance"
    cells: list[dict[str, Any]] = []
    cells.append(_md(key, len(cells), [
        "# Locy Neural Use Case: Predictive Maintenance (Rust)",
        "",
        "Compact Rust counterpart to the Python predictive-maintenance flagship. Same Phase D capability surface — `CREATE MODEL` + `CALIBRATE` + `VALIDATE` + `EXPLAIN` — driving a small inline equipment graph through a `MockClassifier` rather than a registered Python callable.",
    ]))
    cells.append(_md(key, len(cells), [
        "## How To Read This Notebook",
        "",
        "- Same conceptual shape as the Python flagship; mock classifier inline.",
        "- Schema declared first, then seeded data, then the Locy program.",
        "- Read the EXPLAIN output to see `NeuralProvenance` per derivation.",
    ]))
    cells.append(_md(key, len(cells), ["## 1) Setup"]))
    cells.append(_code(key, len(cells), [
        "use std::sync::Arc;",
        "use uni_db::{DataType, Uni, Result};",
        "use uni_locy::{LocyConfig, MockClassifier, NeuralClassifier, FeatureValue};",
        "",
        "let db = Uni::in_memory().build().await?;",
    ]))
    cells.append(_md(key, len(cells), ["## 2) Schema"]))
    cells.append(_code(key, len(cells), [
        "db.schema()",
        "    .label(\"Equipment\")",
        "        .property(\"equipment_id\", DataType::String)",
        "        .property(\"air_temp\", DataType::Float64)",
        "        .property(\"actual_failed\", DataType::Bool)",
        "    .done()",
        "    .apply()",
        "    .await?;",
    ]))
    cells.append(_md(key, len(cells), ["## 3) Seed Inline"]))
    cells.append(_code(key, len(cells), [
        "let session = db.session();",
        "let tx = session.tx().await?;",
        "// 8 equipment instances, 3 actually failed (higher air_temp).",
        "let rows: &[(&str, f64, bool)] = &[",
        "    (\"e01\", 301.0, true), (\"e02\", 298.4, false),",
        "    (\"e03\", 298.6, false), (\"e04\", 300.5, true),",
        "    (\"e05\", 298.7, false), (\"e06\", 299.0, false),",
        "    (\"e07\", 300.8, true), (\"e08\", 298.5, false),",
        "];",
        "for (eid, t, failed) in rows {",
        "    let q = format!(",
        "        \"CREATE (:Equipment {{equipment_id: '{}', air_temp: {}, actual_failed: {}}})\",",
        "        eid, t, failed",
        "    );",
        "    tx.execute(&q).await?;",
        "}",
        "tx.commit().await?;",
    ]))
    cells.append(_md(key, len(cells), [
        "## 4) Register the Classifier",
        "",
        "`MockClassifier::new` takes a closure `Fn(&ClassifyInput) -> f64`. The classifier reads the value bound under the model's INPUT name and emits an intentionally over-confident probability so `CALIBRATE` has measurable work to do.",
    ]))
    cells.append(_code(key, len(cells), [
        "let classifier: Arc<dyn NeuralClassifier> = Arc::new(",
        "    MockClassifier::new(\"failure_likelihood\", |inp| {",
        "        let air = match inp.features.get(\"e\") {",
        "            Some(FeatureValue::Float(v)) => *v,",
        "            _ => 0.0,",
        "        };",
        "        let z = (air - 298.5) * 1.5 - 1.0;",
        "        let p = 1.0 / (1.0 + (-z).exp());",
        "        let p_sharp = 1.0 / (1.0 + (-3.0 * (p - 0.5)).exp());",
        "        p_sharp.clamp(0.0, 1.0)",
        "    })",
        ");",
        "let mut config = LocyConfig::default();",
        "config.classifier_registry.insert(\"failure_likelihood\".to_string(), classifier);",
    ]))
    cells.append(_md(key, len(cells), ["## 5) CREATE MODEL + Score"]))
    cells.append(_code(key, len(cells), [
        "let program = r#\"",
        "CREATE MODEL failure_likelihood AS",
        "  INPUT (e)",
        "  FEATURES e.air_temp",
        "  OUTPUT PROB will_fail",
        "  USING xervo('classify/failure-likelihood-v1')",
        "",
        "CREATE RULE asset_risk AS",
        "  MATCH (e:Equipment)",
        "  YIELD KEY e, failure_likelihood(e.air_temp) AS risk",
        "\"#;",
        "let result = session.locy_with(program).with_config(config.clone()).run().await?;",
        "let asset_rows = result.derived_rules().await?.get(\"asset_risk\").map(|v| v.len()).unwrap_or(0);",
        "println!(\"Scored {} equipment\", asset_rows);",
    ]))
    cells.append(_md(key, len(cells), ["## 6) CALIBRATE + VALIDATE"]))
    cells.append(_code(key, len(cells), [
        "let calibrate_program = r#\"",
        "CREATE MODEL failure_likelihood AS",
        "  INPUT (e)",
        "  FEATURES e.air_temp",
        "  OUTPUT PROB will_fail",
        "  USING xervo('classify/failure-likelihood-v1')",
        "",
        "CALIBRATE failure_likelihood",
        "  ON MATCH (e:Equipment)",
        "  TARGET e.actual_failed",
        "  METHOD platt_scaling",
        "\"#;",
        "let calib = session.locy_with(calibrate_program).with_config(config.clone()).run().await?;",
        "println!(\"command_results: {} entries\", calib.command_results().len());",
    ]))
    cells.append(_md(key, len(cells), [
        "## 7) EXPLAIN One High-Risk Asset",
        "",
        "Returns the derivation tree, including a `NeuralProvenance` leaf per classifier invocation (`model_name`, `raw_probability`, `calibrated_probability`, feature dict).",
    ]))
    cells.append(_code(key, len(cells), [
        "let explain_program = format!(\"{}{}\", program, r#\"",
        "",
        "EXPLAIN RULE asset_risk WHERE e.equipment_id = 'e01'",
        "\"#);",
        "let explain = session.locy_with(&explain_program).with_config(config).run().await?;",
        "println!(\"EXPLAIN command_results: {}\", explain.command_results().len());",
    ]))
    return _wrap(cells)


def _adr_notebook() -> dict[str, Any]:
    key = "rust:locy_adverse_drug_reaction"
    cells: list[dict[str, Any]] = []
    cells.append(_md(key, len(cells), [
        "# Locy Neural Use Case: Adverse Drug Reaction Signal Detection (Rust)",
        "",
        "Compact Rust counterpart to the Python ADR flagship. Same shape: an inline pharmacovigilance graph + a `MockClassifier` registered under the model name + `CREATE MODEL` + `CALIBRATE` + `VALIDATE` + `EXPLAIN`.",
    ]))
    cells.append(_md(key, len(cells), ["## 1) Setup + Schema"]))
    cells.append(_code(key, len(cells), [
        "use std::sync::Arc;",
        "use uni_db::{DataType, Uni, Result};",
        "use uni_locy::{LocyConfig, MockClassifier, NeuralClassifier, FeatureValue};",
        "",
        "let db = Uni::in_memory().build().await?;",
        "db.schema()",
        "    .label(\"Report\")",
        "        .property(\"report_id\", DataType::String)",
        "        .property(\"report_count\", DataType::Float64)",
        "        .property(\"is_signal\", DataType::Bool)",
        "    .done()",
        "    .apply()",
        "    .await?;",
    ]))
    cells.append(_md(key, len(cells), ["## 2) Seed 12 Reports (4 Signals)"]))
    cells.append(_code(key, len(cells), [
        "let session = db.session();",
        "let tx = session.tx().await?;",
        "let rows: &[(&str, f64, bool)] = &[",
        "    (\"R01\", 9.2, true),  (\"R02\", 2.0, false),",
        "    (\"R03\", 8.7, true),  (\"R04\", 1.6, false),",
        "    (\"R05\", 2.4, false), (\"R06\", 3.0, false),",
        "    (\"R07\", 9.5, true),  (\"R08\", 2.2, false),",
        "    (\"R09\", 8.9, true),  (\"R10\", 2.6, false),",
        "    (\"R11\", 3.1, false), (\"R12\", 2.0, false),",
        "];",
        "for (rid, rc, is_signal) in rows {",
        "    let q = format!(",
        "        \"CREATE (:Report {{report_id: '{}', report_count: {}, is_signal: {}}})\",",
        "        rid, rc, is_signal",
        "    );",
        "    tx.execute(&q).await?;",
        "}",
        "tx.commit().await?;",
    ]))
    cells.append(_md(key, len(cells), ["## 3) Register the Signal Classifier"]))
    cells.append(_code(key, len(cells), [
        "let classifier: Arc<dyn NeuralClassifier> = Arc::new(",
        "    MockClassifier::new(\"signal_score\", |inp| {",
        "        let rc = match inp.features.get(\"r\") {",
        "            Some(FeatureValue::Float(v)) => *v,",
        "            _ => 0.0,",
        "        };",
        "        let z = (rc - 5.0) * 0.9 - 0.5;",
        "        let p = 1.0 / (1.0 + (-z).exp());",
        "        let p_sharp = 1.0 / (1.0 + (-3.5 * (p - 0.5)).exp());",
        "        p_sharp.clamp(0.0, 1.0)",
        "    })",
        ");",
        "let mut config = LocyConfig::default();",
        "config.classifier_registry.insert(\"signal_score\".to_string(), classifier);",
    ]))
    cells.append(_md(key, len(cells), ["## 4) CREATE MODEL + Score"]))
    cells.append(_code(key, len(cells), [
        "let program = r#\"",
        "CREATE MODEL signal_score AS",
        "  INPUT (r)",
        "  FEATURES r.report_count",
        "  OUTPUT PROB credibility",
        "  USING xervo('classify/adr-signal-v1')",
        "",
        "CREATE RULE scored_reports AS",
        "  MATCH (r:Report)",
        "  YIELD KEY r, signal_score(r.report_count) AS credibility",
        "\"#;",
        "let result = session.locy_with(program).with_config(config.clone()).run().await?;",
        "let n = result.derived_rules().await?.get(\"scored_reports\").map(|v| v.len()).unwrap_or(0);",
        "println!(\"Scored {} reports\", n);",
    ]))
    cells.append(_md(key, len(cells), ["## 5) CALIBRATE Against Held-Out Signal Labels"]))
    cells.append(_code(key, len(cells), [
        "let calibrate_program = r#\"",
        "CREATE MODEL signal_score AS",
        "  INPUT (r)",
        "  FEATURES r.report_count",
        "  OUTPUT PROB credibility",
        "  USING xervo('classify/adr-signal-v1')",
        "",
        "CALIBRATE signal_score",
        "  ON MATCH (r:Report)",
        "  TARGET r.is_signal",
        "  METHOD platt_scaling",
        "\"#;",
        "let calib = session.locy_with(calibrate_program).with_config(config.clone()).run().await?;",
        "println!(\"command_results: {} entries\", calib.command_results().len());",
    ]))
    cells.append(_md(key, len(cells), ["## 6) EXPLAIN One High-Credibility Report"]))
    cells.append(_code(key, len(cells), [
        "let explain_program = format!(\"{}{}\", program, r#\"",
        "",
        "EXPLAIN RULE scored_reports WHERE r.report_id = 'R01'",
        "\"#);",
        "let explain = session.locy_with(&explain_program).with_config(config).run().await?;",
        "println!(\"EXPLAIN command_results: {}\", explain.command_results().len());",
    ]))
    return _wrap(cells)


def _ddi_notebook() -> dict[str, Any]:
    key = "rust:locy_drug_drug_interaction"
    cells: list[dict[str, Any]] = []
    cells.append(_md(key, len(cells), [
        "# Locy Neural Use Case: Drug-Drug Interaction Risk Scoring (Rust)",
        "",
        "Compact Rust counterpart to the Python DDI flagship. Same shape: inline pairwise-interaction graph + a `MockClassifier` + `CREATE MODEL` + `CALIBRATE` + `VALIDATE` + `EXPLAIN`. In production this is exactly where you'd plug in a small MLP head over precomputed R-GCN drug embeddings.",
    ]))
    cells.append(_md(key, len(cells), ["## 1) Setup + Schema"]))
    cells.append(_code(key, len(cells), [
        "use std::sync::Arc;",
        "use uni_db::{DataType, Uni, Result};",
        "use uni_locy::{LocyConfig, MockClassifier, NeuralClassifier, FeatureValue};",
        "",
        "let db = Uni::in_memory().build().await?;",
        "db.schema()",
        "    .label(\"InteractionRecord\")",
        "        .property(\"pair_id\", DataType::String)",
        "        .property(\"severity\", DataType::Float64)",
        "        .property(\"is_dangerous\", DataType::Bool)",
        "    .done()",
        "    .apply()",
        "    .await?;",
    ]))
    cells.append(_md(key, len(cells), ["## 2) Seed Interaction Records"]))
    cells.append(_code(key, len(cells), [
        "let session = db.session();",
        "let tx = session.tx().await?;",
        "let rows: &[(&str, f64, bool)] = &[",
        "    (\"P01\", 0.55, true),  (\"P02\", 0.18, false),",
        "    (\"P03\", 0.60, true),  (\"P04\", 0.22, false),",
        "    (\"P05\", 0.42, true),  (\"P06\", 0.25, false),",
        "    (\"P07\", 0.65, true),  (\"P08\", 0.20, false),",
        "    (\"P09\", 0.50, true),  (\"P10\", 0.30, false),",
        "];",
        "for (pid, sev, danger) in rows {",
        "    let q = format!(",
        "        \"CREATE (:InteractionRecord {{pair_id: '{}', severity: {}, is_dangerous: {}}})\",",
        "        pid, sev, danger",
        "    );",
        "    tx.execute(&q).await?;",
        "}",
        "tx.commit().await?;",
    ]))
    cells.append(_md(key, len(cells), ["## 3) Register the Interaction Classifier"]))
    cells.append(_code(key, len(cells), [
        "let classifier: Arc<dyn NeuralClassifier> = Arc::new(",
        "    MockClassifier::new(\"interaction_score\", |inp| {",
        "        let sev = match inp.features.get(\"rec\") {",
        "            Some(FeatureValue::Float(v)) => *v,",
        "            _ => 0.0,",
        "        };",
        "        let z = (sev - 0.40) * 8.0 - 0.3;",
        "        let p = 1.0 / (1.0 + (-z).exp());",
        "        let p_sharp = 1.0 / (1.0 + (-4.0 * (p - 0.5)).exp());",
        "        p_sharp.clamp(0.0, 1.0)",
        "    })",
        ");",
        "let mut config = LocyConfig::default();",
        "config.classifier_registry.insert(\"interaction_score\".to_string(), classifier);",
    ]))
    cells.append(_md(key, len(cells), ["## 4) CREATE MODEL + Score"]))
    cells.append(_code(key, len(cells), [
        "let program = r#\"",
        "CREATE MODEL interaction_score AS",
        "  INPUT (rec)",
        "  FEATURES rec.severity",
        "  OUTPUT PROB risk",
        "  USING xervo('classify/ddi-v1')",
        "",
        "CREATE RULE scored_interactions AS",
        "  MATCH (rec:InteractionRecord)",
        "  YIELD KEY rec, interaction_score(rec.severity) AS risk",
        "\"#;",
        "let result = session.locy_with(program).with_config(config.clone()).run().await?;",
        "let n = result.derived_rules().await?.get(\"scored_interactions\").map(|v| v.len()).unwrap_or(0);",
        "println!(\"Scored {} interaction pairs\", n);",
    ]))
    cells.append(_md(key, len(cells), ["## 5) CALIBRATE"]))
    cells.append(_code(key, len(cells), [
        "let calibrate_program = r#\"",
        "CREATE MODEL interaction_score AS",
        "  INPUT (rec)",
        "  FEATURES rec.severity",
        "  OUTPUT PROB risk",
        "  USING xervo('classify/ddi-v1')",
        "",
        "CALIBRATE interaction_score",
        "  ON MATCH (rec:InteractionRecord)",
        "  TARGET rec.is_dangerous",
        "  METHOD platt_scaling",
        "\"#;",
        "let calib = session.locy_with(calibrate_program).with_config(config.clone()).run().await?;",
        "println!(\"command_results: {} entries\", calib.command_results().len());",
    ]))
    cells.append(_md(key, len(cells), ["## 6) EXPLAIN One Dangerous Interaction"]))
    cells.append(_code(key, len(cells), [
        "let explain_program = format!(\"{}{}\", program, r#\"",
        "",
        "EXPLAIN RULE scored_interactions WHERE rec.pair_id = 'P01'",
        "\"#);",
        "let explain = session.locy_with(&explain_program).with_config(config).run().await?;",
        "println!(\"EXPLAIN command_results: {}\", explain.command_results().len());",
    ]))
    return _wrap(cells)


def _render(obj: dict[str, Any]) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False) + "\n"


NOTEBOOKS = [
    ("locy_predictive_maintenance.ipynb", _pdm_notebook),
    ("locy_adverse_drug_reaction.ipynb", _adr_notebook),
    ("locy_drug_drug_interaction.ipynb", _ddi_notebook),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("website/docs/examples/rust"),
    )
    args = parser.parse_args()

    failed = False
    for filename, builder in NOTEBOOKS:
        out = args.output_dir / filename
        nb = builder()
        body = _render(nb)
        if args.check:
            existing = out.read_text(encoding="utf-8") if out.exists() else ""
            if existing != body:
                sys.stderr.write(f"ERROR: {out} does not match generator output\n")
                failed = True
            else:
                print(f"OK: {out}")
        else:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(body, encoding="utf-8")
            print(f"Wrote {out} ({len(nb['cells'])} cells)")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
