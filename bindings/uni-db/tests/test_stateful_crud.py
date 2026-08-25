# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Model-based state machine over the CRUD + transaction surface.

Every other test in this directory is example-based: it performs a fixed
sequence and asserts a fixed outcome. This one generates the sequence and
carries a Python-side model of what the database should contain, asserting they
agree after every step. Hypothesis shrinks a failure to a minimal reproducing
sequence and prints it as runnable code.

Why a state machine, given the surface is already guarded
---------------------------------------------------------
The plan that called for this test justified it as checking the sync/async
symmetry contract. That turned out to be covered already, and exhaustively, by
``test_sync_async_parity.py`` and ``test_stub_drift.py`` -- reflection over the
whole API surface beats generation at any *structural* property, because it
checks every class rather than sampling.

So this is aimed at the thing reflection cannot reach: **behaviour over
sequences**. The bugs this repository actually ships are of that shape --
"correct before flush, wrong after", "MERGE creates a duplicate when the same
key repeats inside one UNWIND batch", "a fork does not see its parent's
uncommitted-to-L1 rows". None of those is visible in a method signature.

The sharpest example is issue #135. ``docs/testing/reverts/issue_135.patch``
records in its own header that reverting the fix is *invisible to the
differential oracle* -- it corrupts both sides identically, so comparing two
execution paths cannot see it. A model can: :meth:`GraphMachine.traversal_matches_model`
goes red on the hydrated property value while
:meth:`GraphMachine.scan_matches_model` stays green on the very same rows.

Two preconditions, measured before this was written
---------------------------------------------------
**The fixture is schemaless, deliberately.** The #135 fix site is planned only
when every requested relationship type is *absent from the schema*. Every
fixture in ``conftest.py`` declares its edge types, so a machine built on one
could not reach the defect at any shape. Measured on a bare ``Uni``: an
``EXPLAIN`` of ``MATCH (a)-[r:EDGE_P]->(b)`` plans through ``TraverseMainByType``
and ``list_edge_types()`` stays empty. That is why nothing here calls
``db.schema()``.

**Committed rows are visible without an explicit flush.** Measured: after a
commit with no ``flush()``, both the writing session and an independent sibling
session see the vertices *and* the edges, with the destination property
hydrated. So visibility does not depend on flush state, and ``flush`` is a plain
rule rather than part of the model's visibility function. Also measured:
``l1_run_count`` stays 0 across a commit and only moves on an explicit
``flush()``, so the dirty-L0 state that #97 needs is genuinely reachable rather
than being flushed away underneath us.

Coverage
--------
A generated machine can pass by never exercising anything. Measured with
``--hypothesis-show-statistics`` on the ``pr`` profile, the share of examples in
which each rule fired at least once: create_vertex 81%, flush 62%,
unwind_merge 58%, detach_delete 50%, completed_tx_rejects 46%, merge_edge 46%,
create_edge 42%, rollback 42%, set_property 42%, commit 38%.

That is worth recording because the first version was far worse -- an explicit
``begin_tx`` rule left ``create_edge`` and ``detach_delete`` at **0%**, because
Hypothesis draws uniformly among *enabled* rules and every write rule was
disabled whenever no transaction happened to be open. If these numbers sag,
suspect a precondition before suspecting the generator.

Measured after #181 and #182 were fixed and the two guards that had been
suppressing roughly 60% of generated deletes were removed, so `detach_delete`'s
50% is real work rather than a skip.

There is no async twin, breaking this directory's naming convention on purpose:
Hypothesis's stateful runner does not drive coroutine rules, and sync/async
parity is already checked exhaustively elsewhere.
"""

from __future__ import annotations

import itertools
import os

import pytest
from hypothesis import HealthCheck, event, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    invariant,
    precondition,
    rule,
)

import uni_db

# ---------------------------------------------------------------------------
# Alphabets
# ---------------------------------------------------------------------------
# Fixed rather than generated. Label *spelling* is not an interesting dimension,
# alphabet *size* is -- a small alphabet makes collisions (two vertices sharing
# a label, two edges sharing a type) common instead of vanishingly rare. And a
# generated identifier interpolated into Cypher produces parse errors, which are
# test bugs wearing the costume of a finding.
LABELS = ("Alpha", "Beta", "Gamma")
EDGE_TYPES = ("EDGE_P", "EDGE_Q")

# Only identifiers are ever interpolated into a query string; every *value* goes
# through params. No floats: float equality across the Arrow round trip is a
# separate concern and would produce false positives here.
vals = st.integers(min_value=-1000, max_value=1000)


settings.register_profile(
    "pr",
    max_examples=25,
    stateful_step_count=12,
    deadline=None,
    database=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
settings.register_profile(
    "nightly",
    max_examples=500,
    stateful_step_count=50,
    deadline=None,
    database=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
settings.load_profile(os.environ.get("UNI_HYPOTHESIS_PROFILE", "pr"))
# `deadline=None` is not tuning: every rule crosses PyO3 into a multi-gigabyte
# debug extension and touches disk, so the 200 ms default measures the build
# profile rather than the database. `database=None` because `.hypothesis/` is a
# shared mutable directory, and CI runs under `-n auto` from a fresh checkout
# where a replay cache is useless anyway.


class GraphMachine(RuleBasedStateMachine):
    """Generated CRUD sequences, checked against a Python-side model."""

    uids = Bundle("uids")

    def __init__(self) -> None:
        super().__init__()
        # Fixtures do not apply to state machines -- Hypothesis constructs this
        # object itself, outside pytest's fixture machinery.
        self.db = uni_db.UniBuilder.temporary().build()
        self.session = self.db.session()
        # An independent sibling, so every step also exercises a second reader.
        self.sibling = self.db.session()

        self.tx = None
        self.dead_tx = None
        # Whether the open transaction has executed any mutation. Distinct from
        # "the model has pending changes": creating a vertex and then deleting
        # it inside one transaction leaves nothing pending but the transaction
        # has still done work, and `is_dirty()` reports the latter.
        self.tx_mutated = False

        # uid -> (label, val). Identity is a synthetic property, never a VID:
        # the codebase's own rule is that identity is content-addressed and VIDs
        # are informational. It also means CREATE's make-a-new-node-per-
        # expression behaviour shows up as a *model divergence* rather than as
        # silent aliasing.
        self.committed_v: dict[int, tuple[str, int]] = {}
        self.pending_v: dict[int, tuple[str, int]] = {}
        self.committed_e: set[tuple[int, int, str]] = set()
        self.pending_e: set[tuple[int, int, str]] = set()
        # Deletions are pending until commit, exactly like creations. Applying
        # them to the committed sets immediately is wrong in a way only a
        # concurrent reader notices: a sibling session still sees a committed
        # vertex that an open transaction has deleted but not yet committed.
        self.pending_del_v: set[int] = set()
        self.pending_del_e: set[tuple[int, int, str]] = set()

        self._uid = itertools.count(1)

        assert self.db.list_edge_types() == [], (
            "the machine must run schemaless -- a declared edge type routes the "
            "traversal away from the code path this test exists to cover"
        )

    def teardown(self) -> None:
        try:
            if self.tx is not None:
                self.tx.rollback()
        except Exception:
            pass
        try:
            self.db.shutdown()
        except Exception:
            pass

    # -- helpers ----------------------------------------------------------

    def _rows(self, query: str) -> list[dict]:
        """Read through the open transaction if there is one, else the session.

        Measured, not assumed: ``tx.query()`` sees the transaction's own
        uncommitted writes while ``session.query()`` does not -- reading through
        the session inside an open transaction returns the committed state only.
        Both halves are checked here: this reader follows the transaction (so
        the invariants assert read-your-own-writes), while
        :meth:`sibling_sees_committed_only` deliberately reads through an
        independent session and asserts the opposite.
        """
        reader = self.tx if self.tx is not None else self.session
        return [r.to_dict() for r in reader.query(query).rows]

    def _ensure_tx(self) -> None:
        """Open a transaction if none is current.

        Write rules call this rather than carrying a `tx is not None`
        precondition. With an explicit `begin_tx` gate, measured coverage was
        dire: only 27% of examples ever opened a transaction, and `create_edge`
        and `detach_delete` never fired once -- Hypothesis draws uniformly among
        *enabled* rules, so every write rule sat disabled for most of the run
        while `flush` and `begin_tx` soaked up the steps. Opening lazily makes
        every write rule always enabled, which is also how the API is actually
        used.
        """
        if self.tx is None:
            self.tx = self.session.tx()
            self.tx_mutated = False
            assert not self.tx.is_dirty()
            assert not self.tx.is_completed()

    _COUNTERS = (
        "nodes_created",
        "nodes_deleted",
        "relationships_created",
        "relationships_deleted",
        "properties_set",
        "labels_added",
        "labels_removed",
    )

    def _exec(self, cypher: str, params: dict | None = None):
        """Run a mutation, recording whether it actually changed anything.

        `tx_mutated` is taken from the result's own counters rather than set on
        every call, because a statement whose MATCH finds nothing is a no-op and
        `is_dirty()` correctly stays False for it. Measured: after
        `MATCH (n {uid: 999}) SET n.val = 1` against an empty database,
        `is_dirty()` is False.
        """
        self._ensure_tx()
        r = self.tx.execute(cypher, params) if params else self.tx.execute(cypher)
        if any(getattr(r, c, 0) for c in self._COUNTERS):
            self.tx_mutated = True
        return r

    @property
    def visible_v(self) -> dict[int, tuple[str, int]]:
        """What the open transaction should see: committed, plus its own writes."""
        v = {**self.committed_v, **self.pending_v}
        for u in self.pending_del_v:
            v.pop(u, None)
        return v

    @property
    def visible_e(self) -> set[tuple[int, int, str]]:
        return (self.committed_e | self.pending_e) - self.pending_del_e

    # -- rules ------------------------------------------------------------

    @rule(target=uids, label=st.sampled_from(LABELS), val=vals)
    def create_vertex(self, label: str, val: int) -> int:
        event("create_vertex")
        uid = next(self._uid)
        self._exec(f"CREATE (:{label} {{uid: {uid}, val: {val}}})")
        self.pending_v[uid] = (label, val)
        return uid

    @rule(a=uids, b=uids, etype=st.sampled_from(EDGE_TYPES))
    def create_edge(self, a: int, b: int, etype: str) -> None:
        event("create_edge")
        # Self-loops are allowed rather than assumed away: they are a real shape,
        # and `assume` in a rule body wastes draws and degrades shrinking.
        self._exec(
            f"MATCH (x {{uid: {a}}}), (y {{uid: {b}}}) CREATE (x)-[:{etype}]->(y)"
        )
        # MATCH semantics: a uid the database no longer has -- rolled back, or
        # deleted -- matches nothing, so no edge is created. The bundle outlives
        # both, so this is reachable and the model must mirror it rather than
        # assume the endpoints exist.
        if a in self.visible_v and b in self.visible_v:
            self.pending_e.add((a, b, etype))

    @rule(u=uids, val=vals)
    def set_property(self, u: int, val: int) -> None:
        event("set_property")
        self._exec(f"MATCH (n {{uid: {u}}}) SET n.val = {val}")
        if u in self.visible_v:
            self.pending_v[u] = (self.visible_v[u][0], val)

    @rule(u=consumes(uids))
    def detach_delete(self, u: int) -> None:
        event("detach_delete")
        self._exec(f"MATCH (n {{uid: {u}}}) DETACH DELETE n")
        self.pending_v.pop(u, None)
        self.pending_del_v.add(u)
        # The cascade is the interesting half: a DETACH DELETE that drops the
        # vertex but strands its edges passes any vertex-only check.
        for e in {e for e in self.visible_e if e[0] == u or e[1] == u}:
            self.pending_e.discard(e)
            self.pending_del_e.add(e)

    @rule(a=uids, b=uids, etype=st.sampled_from(EDGE_TYPES))
    def merge_edge(self, a: int, b: int, etype: str) -> None:
        event("merge_edge")
        self._exec(
            f"MATCH (x {{uid: {a}}}), (y {{uid: {b}}}) MERGE (x)-[:{etype}]->(y)"
        )
        if a in self.visible_v and b in self.visible_v:
            self.pending_e.add((a, b, etype))

    @rule(
        target=uids,
        label=st.sampled_from(LABELS),
        n=st.integers(min_value=1, max_value=4),
        val=vals,
    )
    def unwind_merge(self, label: str, n: int, val: int):
        """MERGE the same key repeatedly inside one UNWIND batch.

        The #69 shape. The fast path builds a per-batch L0 snapshot, and the
        classic defect is creating a duplicate when a key repeats *within* the
        batch -- invisible to any test that merges distinct keys.
        """
        event("unwind_merge")
        uid = next(self._uid)
        rows = [{"uid": uid, "val": val} for _ in range(n)]
        self._exec(
            f"UNWIND $rows AS r MERGE (n:{label} {{uid: r.uid}}) SET n.val = r.val",
            {"rows": rows},
        )
        self.pending_v[uid] = (label, val)
        return uid

    @rule()
    @precondition(lambda self: self.tx is not None)
    def commit(self) -> None:
        event("commit")
        assert self.tx.is_dirty() == self.tx_mutated, (
            "is_dirty disagrees with whether the transaction executed a mutation"
        )
        self.tx.commit()
        self.committed_v.update(self.pending_v)
        self.committed_e |= self.pending_e
        for u in self.pending_del_v:
            self.committed_v.pop(u, None)
        self.committed_e -= self.pending_del_e
        self.pending_v, self.pending_e = {}, set()
        self.pending_del_v, self.pending_del_e = set(), set()
        self.tx_mutated = False
        self.dead_tx, self.tx = self.tx, None

    @rule()
    @precondition(lambda self: self.tx is not None)
    def rollback(self) -> None:
        event("rollback")
        self.tx.rollback()
        # The discriminating case: a rollback that silently committed would pass
        # every count check that follows a commit.
        self.pending_v, self.pending_e = {}, set()
        self.pending_del_v, self.pending_del_e = set(), set()
        self.tx_mutated = False
        self.dead_tx, self.tx = self.tx, None

    @rule()
    @precondition(lambda self: self.dead_tx is not None)
    def completed_tx_rejects_everything(self) -> None:
        event("completed_tx_rejects")
        assert self.dead_tx.is_completed()
        with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
            self.dead_tx.commit()
        with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
            self.dead_tx.rollback()

    @rule()
    @precondition(lambda self: self.tx is None)
    def flush(self) -> None:
        # A rule rather than something done eagerly after each commit. Flushing
        # eagerly would make the #97 and #135 families unreachable by
        # construction -- the resulting oracle would be wider, greener, and
        # toothless. Nothing here asserts that unflushed data is *invisible*;
        # the contract is that committed data is visible either way, so the
        # model's visibility rule stays trivial.
        event("flush")
        self.db.flush()

    # -- invariants -------------------------------------------------------

    @invariant()
    def scan_matches_model(self) -> None:
        """Per-label scans agree with the model."""
        expected: dict[str, dict[int, int]] = {lbl: {} for lbl in LABELS}
        for uid, (label, val) in self.visible_v.items():
            expected[label][uid] = val
        for label in LABELS:
            rows = self._rows(f"MATCH (n:{label}) RETURN n.uid AS uid, n.val AS val")
            got = {r["uid"]: r["val"] for r in rows}
            assert got == expected[label], f"label {label}: {got} != {expected[label]}"

    @invariant()
    def traversal_matches_model(self) -> None:
        """Traversals agree with the model, *including the hydrated property*.

        The ``dval`` column is the reason this invariant exists separately from
        :meth:`scan_matches_model`. ``docs/testing/reverts/issue_135.patch``
        nulls exactly this hydration, and its header records that the revert is
        invisible to a differential oracle because it corrupts both sides
        identically. Here it is a plain mismatch against the model: the edge set
        still agrees while every ``dval`` comes back None.
        """
        for etype in EDGE_TYPES:
            rows = self._rows(
                f"MATCH (a)-[r:{etype}]->(b) "
                "RETURN a.uid AS s, b.uid AS d, b.val AS dval"
            )
            got = {(r["s"], r["d"]) for r in rows}
            expected = {(s, d) for (s, d, t) in self.visible_e if t == etype}
            assert got == expected, f"{etype}: {got} != {expected}"
            for r in rows:
                assert r["dval"] == self.visible_v[r["d"]][1], (
                    f"{etype}: destination property not hydrated for uid "
                    f"{r['d']}: got {r['dval']!r}, model says "
                    f"{self.visible_v[r['d']][1]!r}"
                )

    @invariant()
    def sibling_sees_committed_only(self) -> None:
        """An independent session sees committed rows, never pending ones."""
        expected = {uid: val for uid, (_, val) in self.committed_v.items()}
        rows = [
            r.to_dict()
            for r in self.sibling.query(
                "MATCH (n) WHERE n.uid IS NOT NULL RETURN n.uid AS uid, n.val AS val"
            ).rows
        ]
        got = {r["uid"]: r["val"] for r in rows}
        assert got == expected, f"sibling session: {got} != {expected}"


GraphMachine.TestCase.settings = settings.get_profile(
    os.environ.get("UNI_HYPOTHESIS_PROFILE", "pr")
)
TestGraphMachine = GraphMachine.TestCase


def test_delete_before_flush_survives_the_next_flush() -> None:
    """Create, delete, flush -- with no flush in between -- must not error.

    The trigger is flush state, not the transaction boundary. All of these
    reproduce it: same transaction or two transactions, schemaless or with the
    label declared, ``CREATE`` or ``MERGE``, with or without ``UNWIND``.

    Two things do *not* reproduce, which is what localises it: inserting a flush
    between the create and the delete, and deleting a vertex that never existed
    on a virgin database. So the tombstone has somewhere to go once the table
    has been materialised, and no tombstone is emitted at all when nothing
    matched.

    Fixed in #182. Kept as a named regression alongside the Rust-side
    `repro_issue_182_delete_before_first_flush`, because this is the shape the
    machine actually generated and the one a binding user would hit.
    """
    db = uni_db.UniBuilder.temporary().build()
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (:Alpha {uid: 1, val: 0})")
    tx.commit()
    tx = s.tx()
    tx.execute("MATCH (n {uid: 1}) DETACH DELETE n")
    tx.commit()
    db.flush()


def test_flush_does_not_resurrect_a_detached_edge() -> None:
    """A flush must not change query results.

    Found by :class:`GraphMachine` at the nightly profile as
    ``EDGE_P: {(6, 6), (2, None)} != {(6, 6)}`` -- a row whose destination reads
    ``None`` because the vertex it points at is deleted.

    The severe half of the pair with #182: that one raised loudly, this one
    silently returned a wrong answer, and it is in the "correct before flush,
    wrong after" family the fork and traversal fixes keep landing in.

    Fixed in #181. Kept for the same reason as its sibling above.
    """
    db = uni_db.UniBuilder.temporary().build()
    s = db.session()
    tx = s.tx()
    tx.execute("CREATE (a {uid: 1}), (b {uid: 2}), (a)-[:EDGE_P]->(b)")
    tx.commit()
    db.flush()

    tx = s.tx()
    tx.execute("MATCH (n {uid: 2}) DETACH DELETE n")
    tx.commit()

    edges = "MATCH (a)-[r:EDGE_P]->(b) RETURN a.uid AS s, b.uid AS d"
    assert [r.to_dict() for r in s.query(edges).rows] == []
    db.flush()
    assert [r.to_dict() for r in s.query(edges).rows] == []
