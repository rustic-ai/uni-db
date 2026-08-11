"""Issue #167 — a temporary database must not outlive the process that made it.

`Uni.temporary()` used to leave its ``$TMPDIR/uni_mem_*`` directory behind on
most clean exits. `Drop for Uni` sends a shutdown broadcast and awaits nothing,
so the directory was removed and then the auto-flush task — waking on that very
same broadcast to perform its final flush — recreated it. Measured here at
20/20 before the fix and 0/20 after.

The check runs in child processes because the failure is about *process exit*:
asserting inside this interpreter would test a different thing entirely.
"""

import glob
import os
import subprocess
import sys
import tempfile

import uni_db

RUNS = 20

# Each variant is a complete child program. The bare form is the one from the
# issue: no `with` block and no explicit `shutdown()`, so teardown reaches only
# `Drop`. The schema form is included because the issue reports the rate is
# unchanged by it, which is what rules out "the database was never really
# opened" as an explanation.
VARIANTS = {
    "bare": "import uni_db; db = uni_db.Uni.temporary()",
    "schema": (
        "import uni_db; db = uni_db.Uni.temporary(); "
        "db.schema().label('X').property('n','string').done().apply()"
    ),
}


def _survivors(tmpdir: str, source: str) -> set:
    """Run `source` in a child with its own TMPDIR; return new uni_mem_* dirs."""
    before = set(glob.glob(os.path.join(tmpdir, "uni_mem_*")))
    env = {**os.environ, "TMPDIR": tmpdir}
    proc = subprocess.run(
        [sys.executable, "-c", source], env=env, capture_output=True, text=True
    )
    assert proc.returncode == 0, proc.stderr
    return set(glob.glob(os.path.join(tmpdir, "uni_mem_*"))) - before


def test_temporary_database_leaves_nothing_behind_on_clean_exit():
    # A private TMPDIR, so a concurrent test's databases cannot be miscounted
    # and so this neither depends on nor disturbs the state of /tmp.
    with tempfile.TemporaryDirectory(prefix="uni_leak_probe_") as tmpdir:
        stranded = {}
        for label, source in VARIANTS.items():
            leaked = set()
            for _ in range(RUNS):
                leaked |= _survivors(tmpdir, source)
            if leaked:
                stranded[label] = sorted(os.path.basename(p) for p in leaked)
        assert not stranded, (
            f"temporary databases survived a clean exit (out of {RUNS} runs each): "
            f"{stranded}"
        )


def test_uri_exposes_the_scratch_directory_root():
    """The path must be reachable from Python, and be the root — not a child.

    Without this a caller who never reaches the context-manager teardown has no
    way to account for the directory except globbing ``$TMPDIR``, which is
    unsafe when another process owns one. It must also be the scratch *root*:
    the storage subdirectory would be useless for cleaning up.
    """
    db = uni_db.Uni.temporary()
    uri = db.uri
    assert os.path.isdir(uri), f"uri {uri!r} is not an existing directory"
    assert os.path.basename(uri).startswith("uni_mem_"), (
        f"uri {uri!r} should be the uni_mem_* scratch root, not a child of it"
    )
