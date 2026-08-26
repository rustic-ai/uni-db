#!/usr/bin/env python3
"""Fetch and verify the large benchmark fixtures pinned in fixtures.toml.

Exit codes:
    0  every selected fixture is present and verified
    1  a download or a verification failed
    2  usage error, or a manifest that cannot be trusted

Why this exists
---------------

Track E's benchmarks (LDBC SNB, ann-benchmarks, BEIR) need corpora that are too
large to commit. Before this script the repo had three unrelated ad-hoc ways to
obtain an external asset and **no checksum verification of any download
anywhere** -- the ONNX Runtime tarball is ``curl``ed unverified in
``.github/workflows/ci.yml``, and ``website/scripts/prepare_*.py`` treat
``st_size > 1000`` as an integrity check.

What "verified" means here
--------------------------

A digest this script computes after downloading, and then writes into our own
manifest, would verify **nothing**. That is the self-certifying case
``crates/uni-plugin/src/verify.rs`` already warns about in prose: an attacker
who can rewrite the payload can rewrite the digest beside it.

So the digest arrives from outside the artifact, and three independent channels
must agree:

* **A** -- sha256 (or git blob sha1) of the bytes on disk, computed here.
* **B** -- the pin in ``fixtures.toml``, reviewed by a human in a git commit.
* **C** -- what the Hub publishes for that path at that revision. For LFS files
  the tree API's ``lfs.oid`` *is* the sha256 of the content, computed server
  side at upload time on a different machine through different code. For small
  non-LFS files it is the git blob sha1, which is equally recomputable. Channel
  C is checked by ``--check-upstream``.

A and B together give **transport integrity** and **immutability pinning** --
"these are the bytes a human reviewed in commit X", which is what makes a
published benchmark number reproducible later. Neither gives **provenance**;
that a corpus really is the publisher's is not something a hash we chose can
establish, and this script does not claim it.

Why sha256 and not blake3
-------------------------

blake3 is this workspace's internal integrity primitive (the WAL envelope, the
plugin payload pin). It is deliberately not used here: Python's stdlib cannot
compute it, so adopting it would mean a second implementation and guaranteed
drift, and sha256 is what the Hub natively publishes. Matching the external
source is worth more than matching the WAL.

Why Python and not the hf-hub crate
-----------------------------------

The Hub serves LFS content via a 302 to an **absolute** URL on a different host
(``us.aws.cdn.hf.co``). ``hf-hub`` 0.5.0's ``relative_redirect_client`` refuses
absolute redirects and returns 404 -- a live failure already documented at
``crates/uni/tests/reranker_integration.rs`` and worked around in CI by
filtering a test out by name. ``urllib.request`` follows it. Fetching from Rust
would walk straight into a known-broken path.

Usage:
    fetch.py                                   # fetch + verify everything
    fetch.py --only beir-scifact-corpus        # one fixture (repeatable)
    fetch.py --check                           # verify warm cache, never network
    fetch.py --check-upstream                  # assert pins still match the Hub
    fetch.py --print-path --only NAME          # resolved local path, for consumers
    fetch.py --emit-entry REPO PATH --revision SHA   # a TOML block to paste
    fetch.py --self-test                       # exercise the verify path offline
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
MANIFEST = REPO / "scripts" / "fixtures" / "fixtures.toml"

SCHEMA = 1
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_LEN = {"sha256": 64, "sha1-git": 40}
REQUIRED = ("name", "repo", "repo_type", "revision", "path", "digest_algo", "digest", "bytes")

# Read in 1 MiB blocks: large enough that hashing dominates syscalls, small
# enough that a multi-GB fixture never lands in memory.
CHUNK = 1 << 20


# --------------------------------------------------------------------------
# cache location
# --------------------------------------------------------------------------


def cache_root() -> Path:
    """Resolve the fixture cache root.

    Deliberately outside the source tree. ``crates/uni/tests/common/bge_m3_real_onnx.rs``
    records that a relative default previously stranded a 2.1 GB model inside the
    checkout, and ``cargo clean`` must not destroy a multi-GB download.

    There is no fallback to the working directory: a missing HOME is an error,
    not a reason to litter wherever the process happens to be standing.
    """
    if env := os.environ.get("UNI_FIXTURE_DIR"):
        return Path(env)
    if xdg := os.environ.get("XDG_CACHE_HOME"):
        return Path(xdg) / "uni-fixtures"
    if home := os.environ.get("HOME"):
        return Path(home) / ".cache" / "uni-fixtures"
    raise SystemExit("none of UNI_FIXTURE_DIR, XDG_CACHE_HOME or HOME is set; cannot place the cache")


def local_path(root: Path, entry: dict) -> Path:
    """``<root>/<name>/<revision>/<basename>``.

    The revision is in the path so a re-pin never silently reuses the old bytes
    and two pinned revisions can coexist.
    """
    return root / entry["name"] / entry["revision"] / Path(entry["path"]).name


def receipt_path(root: Path, entry: dict) -> Path:
    return local_path(root, entry).parent / "receipt.json"


# --------------------------------------------------------------------------
# manifest
# --------------------------------------------------------------------------


def load_manifest(path: Path) -> list[dict]:
    """Parse and fully validate the manifest, or exit 2.

    Every check here is a refusal to proceed on an unverifiable pin. In
    particular a missing digest is an error rather than an unverified download:
    ``verify_hash_pin`` in ``crates/uni-plugin/src/verify.rs`` returns ``Ok(())``
    when its pin is absent, and that fail-open is exactly what this must not
    copy.
    """
    if not path.exists():
        raise SystemExit(f"no manifest at {path}")
    doc = tomllib.loads(path.read_text())
    if doc.get("schema") != SCHEMA:
        raise SystemExit(f"unsupported manifest schema: {doc.get('schema')!r} (expected {SCHEMA})")

    entries = doc.get("fixture") or []
    if not entries:
        raise SystemExit(f"{path} declares no fixtures")

    seen: set[str] = set()
    for i, e in enumerate(entries):
        where = f"fixture[{i}]" + (f" ({e['name']})" if "name" in e else "")
        for key in REQUIRED:
            if key not in e:
                raise SystemExit(f"{where}: missing required key {key!r}")
        if e["name"] in seen:
            raise SystemExit(f"{where}: duplicate fixture name {e['name']!r}")
        seen.add(e["name"])
        if not REVISION_RE.match(e["revision"]):
            raise SystemExit(
                f"{where}: revision {e['revision']!r} is not a 40-hex commit sha. "
                "A branch name moves, which makes a pinned digest fail spuriously and a "
                "published benchmark number unreproducible."
            )
        algo = e["digest_algo"]
        if algo not in DIGEST_LEN:
            raise SystemExit(f"{where}: unknown digest_algo {algo!r} (want one of {sorted(DIGEST_LEN)})")
        if not re.fullmatch(rf"[0-9a-f]{{{DIGEST_LEN[algo]}}}", e["digest"]):
            raise SystemExit(f"{where}: digest is not {DIGEST_LEN[algo]} hex chars for {algo}")
        if not isinstance(e["bytes"], int) or e["bytes"] <= 0:
            raise SystemExit(f"{where}: bytes must be a positive integer")
        if e["repo_type"] not in ("dataset", "model"):
            raise SystemExit(f"{where}: repo_type must be 'dataset' or 'model'")
        # `file://` is reachable only from --self-test, never from a manifest;
        # otherwise it would be a way to point verification at a local file and
        # bypass the download path entirely.
        if "://" in e["repo"] or e["repo"].startswith("file"):
            raise SystemExit(f"{where}: repo must be an 'org/name' slug, not a URL")
    return entries


def select(entries: list[dict], only: list[str]) -> list[dict]:
    if not only:
        return entries
    by_name = {e["name"]: e for e in entries}
    unknown = [n for n in only if n not in by_name]
    if unknown:
        raise SystemExit(f"no such fixture(s): {', '.join(unknown)}. Known: {', '.join(sorted(by_name))}")
    return [by_name[n] for n in only]


# --------------------------------------------------------------------------
# digests
# --------------------------------------------------------------------------


def digest_file(path: Path, algo: str) -> str:
    """sha256 of content, or the git blob sha1 the Hub reports for non-LFS files."""
    if algo == "sha256":
        h = hashlib.sha256()
    elif algo == "sha1-git":
        h = hashlib.sha1()
        h.update(b"blob %d\0" % path.stat().st_size)
    else:
        raise SystemExit(f"unknown digest algo {algo!r}")
    with path.open("rb") as fh:
        while block := fh.read(CHUNK):
            h.update(block)
    return h.hexdigest()


def verify_file(path: Path, entry: dict) -> str | None:
    """Return None when the file matches the pin, else a human-readable reason.

    Size is checked because it is a cheap discriminator, but it is never *the*
    check -- ``website/scripts/prepare_*.py`` treating ``st_size > 1000`` as
    integrity is the bug this exists to not repeat.
    """
    if not path.exists():
        return "absent"
    actual_bytes = path.stat().st_size
    if actual_bytes != entry["bytes"]:
        return f"size {actual_bytes} != pinned {entry['bytes']}"
    actual = digest_file(path, entry["digest_algo"])
    if actual != entry["digest"]:
        return f"{entry['digest_algo']} {actual} != pinned {entry['digest']}"
    return None


# --------------------------------------------------------------------------
# network
# --------------------------------------------------------------------------


def hub_url(entry: dict) -> str:
    prefix = "datasets/" if entry["repo_type"] == "dataset" else ""
    return f"https://huggingface.co/{prefix}{entry['repo']}/resolve/{entry['revision']}/{entry['path']}"


def hub_api_tree(entry: dict) -> list[dict]:
    prefix = "datasets" if entry["repo_type"] == "dataset" else "models"
    url = f"https://huggingface.co/api/{prefix}/{entry['repo']}/tree/{entry['revision']}?recursive=1"
    req = urllib.request.Request(url, headers=auth_headers())
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def auth_headers() -> dict[str, str]:
    """HF_TOKEN is optional by design.

    The anonymous path is the one CI exercises; if a token ever became required
    the layer would work only on the machine that has one.
    """
    headers = {"User-Agent": "uni-db-fixtures/1"}
    if token := os.environ.get("HF_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    return headers


def download(entry: dict, dest: Path) -> None:
    """Stream to a temp file beside dest, verify, then atomically rename.

    Nothing lands at ``dest`` until it has matched the pin, so an interrupted or
    corrupt transfer can never be mistaken for a warm cache on the next run.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    free = shutil.disk_usage(dest.parent).free
    if free < entry["bytes"] * 2:
        raise SystemExit(f"{dest.parent}: {free} bytes free, need ~{entry['bytes'] * 2} for {entry['name']}")

    headers = auth_headers()
    # Refuse transparent compression: the digest covers the stored bytes, and a
    # server-gzipped body would be decompressed by urllib, so the file on disk
    # would never match a digest taken over the compressed stream.
    headers["Accept-Encoding"] = "identity"
    req = urllib.request.Request(hub_url(entry), headers=headers)

    tmp = dest.parent / f".{dest.name}.part.{os.getpid()}"
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            if resp.status != 200:
                raise SystemExit(f"{entry['name']}: HTTP {resp.status} from {resp.url}")
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
            # A gated or renamed repo answers 200 with an HTML login/error page.
            # Writing that to disk under a .parquet name is the classic way a
            # fetch "succeeds" while obtaining nothing.
            if ctype in ("text/html", "application/xhtml+xml"):
                raise SystemExit(f"{entry['name']}: server returned {ctype}, not file content ({resp.url})")
            if enc := resp.headers.get("Content-Encoding"):
                raise SystemExit(f"{entry['name']}: server applied Content-Encoding {enc!r}; refusing")
            declared = resp.headers.get("Content-Length")
            if declared is not None and int(declared) != entry["bytes"]:
                raise SystemExit(
                    f"{entry['name']}: server declares {declared} bytes, manifest pins {entry['bytes']}"
                )
            written = 0
            with tmp.open("wb") as fh:
                while block := resp.read(CHUNK):
                    fh.write(block)
                    written += len(block)
        if written != entry["bytes"]:
            raise SystemExit(f"{entry['name']}: received {written} bytes, manifest pins {entry['bytes']}")
        if reason := verify_file(tmp, entry):
            raise SystemExit(f"{entry['name']}: verification failed after download: {reason}")
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def write_receipt(root: Path, entry: dict) -> None:
    """Record what was verified, for the Rust consumer to read.

    The consumer reads this and never the manifest: one implementation of the
    digest logic, in Python, so a Rust twin cannot drift from it.
    """
    receipt_path(root, entry).write_text(
        json.dumps(
            {
                "schema": SCHEMA,
                "name": entry["name"],
                "repo": entry["repo"],
                "revision": entry["revision"],
                "path": entry["path"],
                "digest_algo": entry["digest_algo"],
                "digest": entry["digest"],
                "bytes": entry["bytes"],
                "file": local_path(root, entry).name,
            },
            indent=2,
        )
        + "\n"
    )


# --------------------------------------------------------------------------
# modes
# --------------------------------------------------------------------------


def run_fetch(entries: list[dict], root: Path, network: bool) -> tuple[int, list[tuple[str, str]]]:
    """Fetch (unless --check) and verify. Returns (exit code, per-entry results)."""
    results: list[tuple[str, str]] = []
    failed = False
    for e in entries:
        path = local_path(root, e)
        reason = verify_file(path, e)
        if reason == "absent" and network:
            try:
                download(e, path)
            except (SystemExit, urllib.error.URLError, OSError) as exc:
                print(f"[fixture] FAIL name={e['name']} {exc}", file=sys.stderr)
                results.append((e["name"], f"FAIL: {exc}"))
                failed = True
                continue
            reason = verify_file(path, e)
        if reason is not None:
            # A file that is present but wrong is deleted: leaving it would let
            # the next run mistake it for a warm cache.
            if reason != "absent":
                path.unlink(missing_ok=True)
                print(f"[fixture] removed corrupt {path}", file=sys.stderr)
            hint = "" if network else " (run without --check to download)"
            print(f"[fixture] FAIL name={e['name']} {reason}{hint}", file=sys.stderr)
            results.append((e["name"], f"FAIL: {reason}"))
            failed = True
            continue
        write_receipt(root, e)
        # The positive marker. CI asserts on evidence that work happened, never
        # on the absence of a failure -- a job that silently does nothing prints
        # no marker and so cannot pass a grep for one.
        print(
            f"[fixture] OK name={e['name']} rev={e['revision'][:12]} "
            f"bytes={e['bytes']} {e['digest_algo']}={e['digest'][:16]} path={path}"
        )
        results.append((e["name"], "OK"))
    return (1 if failed else 0), results


def run_check_upstream(entries: list[dict]) -> int:
    """Assert every pin still matches what the Hub publishes at that revision.

    This is the only check that can tell "our pin is stale" apart from "the
    download is corrupt", and the only one that consults a digest we did not
    choose.
    """
    failed = False
    trees: dict[tuple[str, str], dict[str, dict]] = {}
    for e in entries:
        key = (e["repo"], e["revision"])
        if key not in trees:
            try:
                trees[key] = {x["path"]: x for x in hub_api_tree(e) if x.get("type") == "file"}
            except (urllib.error.URLError, OSError) as exc:
                print(f"[fixture] FAIL name={e['name']} cannot reach the Hub: {exc}", file=sys.stderr)
                failed = True
                continue
        node = trees[key].get(e["path"])
        if node is None:
            print(f"[fixture] FAIL name={e['name']} {e['path']} absent at {e['revision'][:12]}", file=sys.stderr)
            failed = True
            continue
        lfs = node.get("lfs") or {}
        published = lfs.get("oid") if lfs else node.get("oid")
        algo = "sha256" if lfs else "sha1-git"
        if algo != e["digest_algo"]:
            print(
                f"[fixture] FAIL name={e['name']} Hub publishes a {algo} digest, manifest pins "
                f"{e['digest_algo']}",
                file=sys.stderr,
            )
            failed = True
            continue
        if published != e["digest"]:
            print(
                f"[fixture] FAIL name={e['name']} manifest {e['digest']} != Hub {published}",
                file=sys.stderr,
            )
            failed = True
            continue
        if node.get("size") != e["bytes"]:
            print(f"[fixture] FAIL name={e['name']} manifest {e['bytes']} != Hub {node.get('size')}", file=sys.stderr)
            failed = True
            continue
        print(f"[fixture] PINNED name={e['name']} {algo}={published[:16]} agrees with the Hub")
    return 1 if failed else 0


def run_emit_entry(repo: str, path: str, revision: str, repo_type: str) -> int:
    """Print a manifest block for a human to paste into a reviewed commit.

    This script never writes the manifest. A fetcher that could edit the pins it
    verifies against would be checking its own homework; the git commit is the
    out-of-band channel that makes the pin mean something.
    """
    if not REVISION_RE.match(revision):
        print(f"--revision must be a 40-hex commit sha, got {revision!r}", file=sys.stderr)
        return 2
    probe = {"repo": repo, "repo_type": repo_type, "revision": revision, "path": path}
    try:
        nodes = {x["path"]: x for x in hub_api_tree(probe) if x.get("type") == "file"}
    except (urllib.error.URLError, OSError) as exc:
        print(f"cannot reach the Hub: {exc}", file=sys.stderr)
        return 1
    node = nodes.get(path)
    if node is None:
        print(f"{path!r} not found in {repo} at {revision[:12]}", file=sys.stderr)
        return 1
    lfs = node.get("lfs") or {}
    algo = "sha256" if lfs else "sha1-git"
    digest = lfs.get("oid") if lfs else node.get("oid")
    print("[[fixture]]")
    print('name        = "CHOOSE-A-NAME"')
    print(f'repo        = "{repo}"')
    print(f'repo_type   = "{repo_type}"')
    print(f'revision    = "{revision}"')
    print(f'path        = "{path}"')
    print(f'digest_algo = "{algo}"')
    print(f'digest      = "{digest}"')
    print(f"bytes       = {node.get('size')}")
    print('license     = "FILL IN"')
    print('consumer    = "FILL IN"')
    print(f'produced_by = "upstream: {repo}"')
    return 0


def run_self_test() -> int:
    """Exercise the verification path offline, against files we build here.

    Nothing under ``scripts/`` has tests -- the house convention is to design
    every silent-pass path into a hard failure and record the validation in the
    commit message. This mode is how that validation is reproducible rather than
    a claim: it proves the checker bites, which is the same discipline C2 applied
    when it pointed a crash test at a non-existent seam to prove the assertion
    could fail.
    """
    cases: list[tuple[str, bool]] = []
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        body = b"scifact-shaped bytes\n" * 100
        good = tmp / "good.bin"
        good.write_bytes(body)
        sha = hashlib.sha256(body).hexdigest()
        entry = {
            "name": "self-test",
            "digest_algo": "sha256",
            "digest": sha,
            "bytes": len(body),
        }

        cases.append(("matching digest verifies", verify_file(good, entry) is None))

        wrong = dict(entry, digest="0" * 64)
        cases.append(("wrong digest is rejected", verify_file(good, wrong) is not None))

        truncated = tmp / "short.bin"
        truncated.write_bytes(body[:-1])
        cases.append(("truncated file is rejected", verify_file(truncated, entry) is not None))

        cases.append(("absent file is reported", verify_file(tmp / "nope.bin", entry) == "absent"))

        # git blob sha1, the digest the Hub publishes for small non-LFS files.
        blob = hashlib.sha1(b"blob %d\0" % len(body) + body).hexdigest()
        gitentry = dict(entry, digest_algo="sha1-git", digest=blob)
        cases.append(("git blob sha1 verifies", verify_file(good, gitentry) is None))
        cases.append(
            ("git blob sha1 rejects a wrong pin", verify_file(good, dict(gitentry, digest="0" * 40)) is not None)
        )

        # Manifest validation: each of these must refuse to load.
        for label, toml_text in [
            ("branch revision refused", 'schema=1\n[[fixture]]\nname="x"\nrepo="a/b"\nrepo_type="dataset"\n'
                                       'revision="main"\npath="f"\ndigest_algo="sha256"\ndigest="' + "0" * 64
                                       + '"\nbytes=1\n'),
            ("missing digest refused", 'schema=1\n[[fixture]]\nname="x"\nrepo="a/b"\nrepo_type="dataset"\n'
                                       'revision="' + "a" * 40 + '"\npath="f"\ndigest_algo="sha256"\nbytes=1\n'),
            ("bad schema refused", 'schema=99\n[[fixture]]\nname="x"\n'),
            ("empty manifest refused", "schema=1\n"),
            ("url as repo refused", 'schema=1\n[[fixture]]\nname="x"\nrepo="file:///etc"\nrepo_type="dataset"\n'
                                    'revision="' + "a" * 40 + '"\npath="f"\ndigest_algo="sha256"\ndigest="'
                                    + "0" * 64 + '"\nbytes=1\n'),
            ("duplicate name refused", 'schema=1\n' + 2 * ('[[fixture]]\nname="x"\nrepo="a/b"\nrepo_type="dataset"\n'
                                       'revision="' + "a" * 40 + '"\npath="f"\ndigest_algo="sha256"\ndigest="'
                                       + "0" * 64 + '"\nbytes=1\n')),
        ]:
            mf = tmp / "m.toml"
            mf.write_text(toml_text)
            try:
                load_manifest(mf)
                cases.append((label, False))
            except SystemExit:
                cases.append((label, True))

    ok = True
    for label, passed in cases:
        print(f"  {'pass' if passed else 'FAIL'}  {label}")
        ok = ok and passed
    print(f"[fixture] self-test: {sum(1 for _, p in cases if p)}/{len(cases)} cases pass")
    return 0 if ok else 1


def markdown_table(results: list[tuple[str, str]]) -> str:
    lines = ["| fixture | result |", "|---|---|"]
    lines += [f"| `{name}` | {status} |" for name, status in results]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", type=Path, default=MANIFEST)
    ap.add_argument("--only", action="append", default=[], metavar="NAME", help="fixture to act on (repeatable)")
    ap.add_argument("--check", action="store_true", help="verify the warm cache; never touch the network")
    ap.add_argument("--check-upstream", action="store_true", help="assert every pin still matches the Hub")
    ap.add_argument("--print-path", action="store_true", help="print the resolved local path and exit")
    ap.add_argument("--emit-entry", nargs=2, metavar=("REPO", "PATH"), help="print a manifest block to paste")
    ap.add_argument("--revision", help="commit sha, required with --emit-entry")
    ap.add_argument("--repo-type", default="dataset", choices=("dataset", "model"))
    ap.add_argument("--self-test", action="store_true", help="exercise the verify path offline")
    ap.add_argument("--markdown", action="store_true", help="emit a markdown table for a step summary")
    args = ap.parse_args()

    if args.self_test:
        return run_self_test()

    if args.emit_entry:
        if not args.revision:
            print("--emit-entry requires --revision", file=sys.stderr)
            return 2
        return run_emit_entry(args.emit_entry[0], args.emit_entry[1], args.revision, args.repo_type)

    try:
        entries = select(load_manifest(args.manifest), args.only)
        root = cache_root()
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.print_path:
        if len(entries) != 1:
            print("--print-path needs exactly one --only NAME", file=sys.stderr)
            return 2
        entry = entries[0]
        path = local_path(root, entry)
        # Verify before printing. Emitting a path that does not exist would hand
        # a consumer something that looks like success and fails later somewhere
        # less legible -- the same shape as `LOCOMO_JSON`'s default, a path
        # nothing in the repo ever creates.
        if reason := verify_file(path, entry):
            print(
                f"[fixture] FAIL name={entry['name']} {reason}\n"
                f"  run: python3 scripts/fixtures/fetch.py --only {entry['name']}",
                file=sys.stderr,
            )
            return 1
        print(path)
        return 0

    if args.check_upstream:
        return run_check_upstream(entries)

    code, results = run_fetch(entries, root, network=not args.check)
    if args.markdown:
        print(markdown_table(results))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
