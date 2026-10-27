# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Constrained what-if evaluation using existing Locy/Cypher, without a new DSL.

Run from bindings/uni-db: ``python examples/locy_constrained_scenarios.py``.
The caller supplies the loop and search metadata; these are not native ABDUCE
features. Each ASSUME runs against the same pinned scratch-transaction baseline.
"""

from __future__ import annotations

import json
from time import monotonic
from typing import Any

from uni_db import LocyResult, Session, Transaction, Uni

ELIGIBILITY = """
CREATE RULE eligible_quote AS
MATCH (q:Quote)-[:APPROVED_FOR]->(o:Offer)
WHERE o.id = $offer_id AND q.active = true
  AND q.qty >= o.qty AND q.currency = o.currency
  AND q.unit_cost_cents > 0
YIELD KEY o, KEY q, 'accept_quote' AS decision,
      q.unit_cost_cents * o.qty AS purchase_total_cents
"""
GOAL = """
CREATE RULE feasible AS
MATCH (o:Offer)
WHERE o.id = $offer_id
  AND o.unit_cost_cents * o.qty <= o.budget_cents
  AND o.delivery_days <= o.deadline_days
YIELD KEY o
"""
# The only supplied IDs come from ELIGIBILITY in this same pinned baseline.
# Consequently, eligibility need not be copied into a second WHERE clause.
# Re-executing a recommendation later requires rechecking eligibility then.
SCENARIO = (
    GOAL
    + """
ASSUME {
  MATCH (o:Offer), (q:Quote)
  WHERE id(o) = $offer_vid AND id(q) = $quote_vid
  SET o.unit_cost_cents = q.unit_cost_cents,
      o.delivery_days = q.delivery_days
} THEN {
  QUERY feasible WHERE id(o) = $offer_vid
  RETURN o.id AS offer_id, o.unit_cost_cents AS unit_cost_cents,
         o.delivery_days AS delivery_days
}
"""
)


def offer_state(reader: Session | Transaction) -> list[dict[str, Any]]:
    return [
        row.to_dict()
        for row in reader.query(
            "MATCH (o:Offer) RETURN o.id AS id, o.unit_cost_cents AS price, "
            "o.delivery_days AS days ORDER BY id"
        ).rows
    ]


def main() -> None:
    db = Uni.in_memory()
    session = db.session()
    seed = session.tx()
    seed.execute(
        "CREATE (:Offer {id:'OFF3',unit_cost_cents:700,qty:120,currency:'USD',"
        "delivery_days:8,deadline_days:5,budget_cents:72000})"
    )
    for quote_id, price, days, qty, active, currency in [
        ("CHEAP_LATE", 500, 9, 120, True, "USD"),
        ("Q1", 590, 4, 120, True, "USD"),
        ("ZERO", 0, 1, 120, True, "USD"),
        ("SHORT", 200, 1, 119, True, "USD"),
        ("INACTIVE", 100, 1, 120, False, "USD"),
        ("FX", 100, 1, 120, True, "EUR"),
    ]:
        seed.execute(
            "MATCH (o:Offer {id:'OFF3'}) "
            "CREATE (:Quote {id:$id,unit_cost_cents:$price,delivery_days:$days,"
            "qty:$qty,active:$active,currency:$currency})-[:APPROVED_FOR]->(o)",
            {
                "id": quote_id,
                "price": price,
                "days": days,
                "qty": qty,
                "active": active,
                "currency": currency,
            },
        )
    seed.commit()

    candidate_limit = 16
    deadline = monotonic() + 30.0
    scratch = session.scratch()

    def evaluate(program: str, params: dict[str, Any]) -> LocyResult:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("scenario-search deadline expired")
        builder = (
            scratch.locy_with(program).timeout(remaining).max_memory(128 * 1024**2)
        )
        for name, value in params.items():
            builder = builder.param(name, value)
        # The default fails on incomplete evaluation. Empty rows below therefore
        # mean a completely evaluated goal did not hold, not a timed-out goal.
        return builder.run()

    try:
        baseline = offer_state(scratch)
        assert (
            evaluate(GOAL + "QUERY feasible RETURN o.id", {"offer_id": "OFF3"}).rows()
            == []
        )
        # One extra returned row detects candidate truncation. LIMIT does not
        # bound the derivation's work; the evaluation's deadline/memory do that.
        candidates = (
            evaluate(
                ELIGIBILITY
                + "QUERY eligible_quote RETURN id(o) AS offer_vid, id(q) AS quote_vid, "
                "q.id AS quote_id, decision, o.currency AS currency, purchase_total_cents "
                f"ORDER BY quote_id, quote_vid LIMIT {candidate_limit + 1}",
                {"offer_id": "OFF3"},
            ).rows()
            or []
        )
        complete = len(candidates) <= candidate_limit
        selected = candidates[:candidate_limit]
        assert {row["quote_id"] for row in selected} == {"CHEAP_LATE", "Q1"}
        evaluated = []
        for candidate in selected:
            params = {
                "offer_id": "OFF3",
                "offer_vid": candidate["offer_vid"],
                "quote_vid": candidate["quote_vid"],
            }
            result = evaluate(SCENARIO, params)
            rows = result.command_results[0].rows
            assert offer_state(scratch) == baseline, (
                "scenario contaminated the next candidate"
            )
            evaluated.append({**candidate, "feasible": bool(rows), "evidence": rows})

        valid = sorted(
            (row for row in evaluated if row["feasible"]),
            key=lambda row: (row["purchase_total_cents"], row["quote_id"]),
        )
        assert [row["quote_id"] for row in valid] == ["Q1"]
        assert valid[0]["purchase_total_cents"] == 70800
        # A cheaper eligible quote fails the delivery goal: rank only AFTER
        # evaluating consequences, not by taking the cheapest candidate first.
        assert not next(row for row in evaluated if row["quote_id"] == "CHEAP_LATE")[
            "feasible"
        ]
        for candidate in reversed(selected):
            result = evaluate(
                SCENARIO,
                {
                    "offer_id": "OFF3",
                    "offer_vid": candidate["offer_vid"],
                    "quote_vid": candidate["quote_vid"],
                },
            )
            assert bool(result.command_results[0].rows) == (
                candidate["quote_id"] == "Q1"
            )
            assert offer_state(scratch) == baseline
        report = {
            "search_complete": complete,
            "stop_reason": "exhausted" if complete else "candidate_limit",
            "candidate_limit": candidate_limit,
            "candidates_evaluated": len(evaluated),
            "verification_rechecks": len(selected),
            "selection_scope": "eligible_quotes"
            if complete
            else "evaluated_prefix_only",
            "evaluations": evaluated,
            "best": valid[0] if valid else None,
            "restoration_checked": True,
            "order_independent": True,
        }
    finally:
        scratch.rollback()
    assert offer_state(session) == [{"id": "OFF3", "price": 700, "days": 8}]
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
