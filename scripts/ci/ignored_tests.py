#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
"""Every `#[ignore]`d Rust test runs somewhere: in a dedicated CI lane, in the
nightly sweep, or -- registered here with a reason -- on demand only.

An ignored test is skipped by every ordinary `cargo nextest run`, so it runs
only if some lane selects it with `--run-ignored`. Lanes used to select by name
(`test(/soak/)`), and anything whose name matched no lane never ran at all:
measured on 2026-10-22, 85 of 165 ignored tests, among them correctness tests,
a test whose API had rotted, and a probe that hung (it exposed a real drop
deadlock the first time it was run).

The nightly `ignored-sweep` job closes that by construction: it runs every
ignored test in the workspace EXCEPT the ones a dedicated lane already runs
(`DEDICATED_LANES`) and the ones registered in `ON_DEMAND`. A new ignored test
therefore runs nightly unless someone writes down why it cannot.

    python3 scripts/ci/ignored_tests.py sweep-filter   # the sweep's -E filter
    python3 scripts/ci/ignored_tests.py check          # static guard (pr.yml)

`check` needs no build: each `ON_DEMAND` entry must name an ignored test that
exists, and each dedicated lane's filter must still appear verbatim in its
workflow (an edited lane filter would otherwise silently drop or double-run
tests in the sweep).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# (workflow file, the lane's `-E '...'` filter exactly as the workflow writes it, the
# packages or binaries the lane runs it over as a nextest filterset). The sweep
# excludes `scope and (filter)`; a lane's filter only covers the packages that
# lane builds, so it must not be excluded anywhere else.
DEDICATED_LANES: list[tuple[str, str, str]] = [
    (
        ".github/workflows/nightly.yml",  # soak
        "(test(/soak/) | test(/stress/)) and not test(/dqp::/)",
        "package(uni-db) | package(uni-store) | package(uni-locy-oracle)",
    ),
    (
        ".github/workflows/nightly.yml",  # metamorphic
        "test(/metamorphic::/) and test(soak) and not test(/dqp::/) and not test(/binding::/)",
        "binary_id(uni-db::integration)",
    ),
    (
        ".github/workflows/nightly.yml",  # binding (five 10k-case shards)
        "test(metamorphic::binding::binding_guard_soak)",
        "binary_id(uni-db::integration)",
    ),
    (
        ".github/workflows/nightly.yml",  # dqp
        "test(/metamorphic::dqp::/) and test(soak)",
        "binary_id(uni-db::integration)",
    ),
    (
        ".github/workflows/nightly.yml",  # cloud (LocalStack)
        "test(/^cloud_integration_test::/)",
        "binary_id(uni-store::integration)",
    ),
    (
        ".github/workflows/nightly.yml",  # cloud (LocalStack)
        "test(/^hybrid_localstack_e2e::/)",
        "binary_id(uni-db::integration)",
    ),
    (
        ".github/workflows/pr.yml",  # failpoint crash suite, --run-ignored all
        "test(/resilience|recovery|durability|crash_harness/)",
        "package(uni-db) | package(uni-store)",
    ),
    (
        ".github/workflows/ci.yml",  # reranker (ONNX), --run-ignored all
        "not test(=test_real_onnx_cross_encoder_reranks_by_relevance)",
        "binary_id(uni-db::reranker_integration)",
    ),
]

# Ignored tests that cannot run unattended: "<binary_id> <test path>" -> why.
# Keep this short. A test that merely is slow, needs a downloadable model, or
# asserts nothing still belongs in the sweep: running it is what catches the
# hang, the panic, or the API it no longer compiles against.
ON_DEMAND: dict[str, str] = {
    "uni-locy-oracle::differential random::teeth_refind_issue_300": (
        "teeth: validates the random-program generator by re-finding the #300 "
        "bugs, so it passes only against a pre-fix engine checkout"
    ),
    "uni-db::integration bugs::repro_issue_168_family_stale_second_handle::"
    "second_handle_observes_fork_created_elsewhere": (
        "asserts behaviour that is unsupported by design (decided 2026-09-15; "
        "see Uni::list_forks), so it fails today; kept as the specification"
    ),
    "uni-db::integration storage::embedding_stack_overflow_test::mistralrs_tests::"
    "test_mistralrs_embeddinggemma_auto_embed": (
        "google/embeddinggemma-300m is a gated model: downloading it needs an "
        "HF_TOKEN whose account accepted the Gemma license (HTTP 401 without)"
    ),
    "uni-db::integration storage::embedding_stack_overflow_test::mistralrs_tests::"
    "test_mistralrs_embeddinggemma_vector_search": (
        "google/embeddinggemma-300m is a gated model: downloading it needs an "
        "HF_TOKEN whose account accepted the Gemma license (HTTP 401 without)"
    ),
    "uni-db::reranker_integration test_real_onnx_cross_encoder_reranks_by_relevance": (
        "its model is served only through a xet-bridge redirect that hf-hub 0.5 "
        "refuses; ci.yml's reranker lane filters it out for the same reason"
    ),
}


def sweep_filter() -> str:
    """The nightly sweep's nextest filterset."""
    excluded = [f"(({scope}) & ({flt}))" for _, flt, scope in DEDICATED_LANES]
    for test_id in ON_DEMAND:
        binary_id, name = test_id.split(" ", 1)
        excluded.append(f"(binary_id({binary_id}) & test(={name}))")
    return "not (" + " | ".join(excluded) + ")"


def ignored_test_exists(name: str) -> bool:
    """Whether a function `name` (the last path segment) under `#[ignore]`
    exists in the workspace's Rust sources."""
    fn = re.compile(rf"\bfn {re.escape(name)}\s*[<(]")
    for path in (ROOT / "crates").rglob("*.rs"):
        text = path.read_text(encoding="utf-8", errors="replace")
        for m in fn.finditer(text):
            # The attributes directly above the function.
            head = text[: m.start()].rsplit("\n", 12)[1:]
            if any("#[ignore" in line for line in head):
                return True
    return False


def check() -> int:
    problems = []
    for workflow, flt, _ in DEDICATED_LANES:
        # The whole quoted argument, so a lane that narrows or widens its
        # filter (`... and not test(x)`) is caught, not matched as a substring.
        if f"-E '{flt}'" not in (ROOT / workflow).read_text(encoding="utf-8"):
            problems.append(
                f"lane filter {flt!r} no longer appears in {workflow}: update "
                "DEDICATED_LANES to the lane's current filter, or the sweep "
                "drops or double-runs its tests"
            )
    for test_id, reason in ON_DEMAND.items():
        if not reason.strip():
            problems.append(f"ON_DEMAND entry {test_id!r} has no reason")
        name = test_id.split(" ", 1)[1].rsplit("::", 1)[-1]
        if not ignored_test_exists(name):
            problems.append(
                f"ON_DEMAND entry {test_id!r} names no #[ignore]d test; remove it"
            )
    if problems:
        print("ignored-test registry drift:\n")
        print("\n".join(f"  - {p}" for p in problems))
        return 1
    print(
        f"OK: {len(DEDICATED_LANES)} dedicated lane filters current; "
        f"{len(ON_DEMAND)} on-demand ignored tests, each with a reason"
    )
    return 0


def main() -> int:
    if sys.argv[1:] == ["sweep-filter"]:
        print(sweep_filter())
        return 0
    if sys.argv[1:] == ["check"]:
        return check()
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
