# Benchmark fixtures

Some benchmarks need corpora too large to commit — LDBC SNB SF1, SIFT-1M, the
BEIR subsets. This document describes how they are obtained and, more
importantly, what "verified" does and does not claim.

- Manifest: `scripts/fixtures/fixtures.toml`
- Fetcher: `scripts/fixtures/fetch.py` (stdlib only, no third-party deps)
- Cache: `$UNI_FIXTURE_DIR`, else `$XDG_CACHE_HOME/uni-fixtures`, else
  `~/.cache/uni-fixtures`. Never inside the source tree.

## Quick start

```bash
python3 scripts/fixtures/fetch.py                      # fetch + verify everything
python3 scripts/fixtures/fetch.py --only beir-scifact-corpus
python3 scripts/fixtures/fetch.py --check              # verify warm cache, no network
python3 scripts/fixtures/fetch.py --check-upstream     # assert pins still match the Hub
python3 scripts/fixtures/fetch.py --self-test          # prove the checker bites, offline
```

Exit codes: `0` verified, `1` download/verification failure, `2` usage error or
an untrustworthy manifest.

## Why this exists

Before it, the repo had three unrelated ways to obtain an external asset and
**no checksum verification of any download anywhere**:

| pattern | integrity check |
|---|---|
| ORT tarball, `curl`ed in `.github/workflows/ci.yml` | none |
| `website/scripts/prepare_*.py` | `st_size > 1000` |
| `LOCOMO_JSON`, `BENCH_DIR` — bring-your-own-file | none; the default path is one nothing in the repo ever creates |

Built a fourth and fifth time by the LDBC and ANN work independently, they would
have diverged and one would have skipped the checksum.

## What "verified" means

A digest we compute after downloading and then write into our own manifest
verifies **nothing**. That is the self-certifying case
`crates/uni-plugin/src/verify.rs` already warns about in prose: *"an attacker
who can rewrite the payload can rewrite the digest beside it."*

So the digest comes from outside the artifact, and three channels must agree:

| channel | what it is | checked by |
|---|---|---|
| **A** | sha256 (or git blob sha1) of the bytes on disk | every run |
| **B** | the pin in `fixtures.toml`, reviewed by a human in a git commit | every run |
| **C** | what the Hub publishes for that path at that revision | `--check-upstream` |

Channel C is the one that makes this more than bookkeeping. For LFS-backed files
the tree API's `lfs.oid` **is** the sha256 of the content, computed server-side
at upload time on a different machine through different code. For small non-LFS
files it is the git blob sha1, equally recomputable. Both are genuine second
sources.

Three claims, stated separately because they are not equally strong:

- **Transport integrity** — truncation, CDN corruption, a gated repo answering
  200 with an HTML login page. Fully covered by A+B.
- **Immutability pinning** — "these are the bytes a human reviewed in commit X."
  Covered by A+B plus the commit-SHA revision. This is what makes a published
  benchmark number reproducible six months later, and it is the strongest claim
  available.
- **Provenance** — that a corpus really is the publisher's. **Not claimed.** A
  hash we chose cannot establish it.

### Why sha256 and not blake3

blake3 is this workspace's internal integrity primitive (the WAL envelope, the
plugin payload pin). It is deliberately not used here: Python's stdlib cannot
compute it, so adopting it would mean a second implementation and guaranteed
drift, and sha256 is what the Hub natively publishes. Matching the external
source is worth more than matching the WAL. The divergence is intentional, not
an oversight.

### Why Python and not the `hf-hub` crate

The Hub serves LFS content via a 302 to an **absolute** URL on a different host
(`us.aws.cdn.hf.co`). `hf-hub` 0.5.0's `relative_redirect_client` refuses
absolute redirects and returns 404 — a live failure already documented at
`crates/uni/tests/reranker_integration.rs` and worked around in CI by filtering
a test out by name. `urllib.request` follows it. Fetching from Rust would walk
straight into a known-broken path.

## Adding a fixture

Never hand-write a digest. Ask the Hub:

```bash
python3 scripts/fixtures/fetch.py --emit-entry BeIR/scifact \
    corpus/corpus-00000-of-00001.parquet \
    --revision b3b5335604bf5ee3c4447671af975ea25143d4f5
```

It prints a manifest block to **stdout** for you to paste, fill in `name`,
`license` and `consumer`, and commit. The fetcher never writes the manifest: a
fetcher that could edit the pins it verifies against would be checking its own
homework, and the reviewed git commit is the out-of-band channel that makes the
pin mean anything.

Get the revision SHA from
`https://huggingface.co/api/datasets/<repo>/revision/main`. A branch name is
rejected at parse time — it moves, which makes a pinned digest fail spuriously
and a published number unreproducible.

`produced_by` records where a fixture came from. For upstream corpora that is
the repo; for fixtures we generate (LDBC SF1) it must be the **exact generator
invocation**, so the fixture can be regenerated and not merely re-downloaded.

If we ever mirror a corpus whose publisher ships no checksum of its own, record
that honestly — `upstream_digest_source = "computed-at-mirror-<date>"` — so a
reviewer sees that provenance is unestablished rather than inferring it from the
presence of a hex string.

## Anti-vacuity

The failure this layer must never have is reporting success while having
obtained nothing. Each guard is exercised by `--self-test` or by the validation
recorded in the commit that introduced it.

| silent-success mode | guard |
|---|---|
| Manifest entry has no digest | exit 2 — never an unverified download |
| Digest is one we computed ourselves | `--check-upstream` asserts it against the Hub |
| Revision is a branch, so content moves under a stable pin | parse-time `^[0-9a-f]{40}$`, exit 2 |
| Partial or truncated download left in the cache | stream to `.part.<pid>`, verify, **then** `os.replace`; nothing reaches the final path unverified |
| Present-but-wrong file mistaken for a warm cache next run | a failed verify **deletes** the file |
| Size heuristic passes a wrong file | size is checked but is never *the* check; the digest is |
| Gated repo answers 200 with an HTML page | `Content-Type: text/html` is rejected before writing |
| Server gzips the body, so the stored bytes never match the digest | `Accept-Encoding: identity`; any `Content-Encoding` is refused |
| Typo'd selector matches nothing and exits 0 | unknown `--only` name → exit 2, listing the known names |
| Empty manifest | exit 2 |
| `--print-path` emits a path for a fixture that is not there | verifies first; on failure prints the fetch command and exits 1 |
| Warm path skips verification for speed | the warm path re-hashes; `--check` is the offline form of exactly that |
| Consumer runs on a missing fixture | see below — hard failure, never a synthetic substitute |

## Consumer contract

**A missing fixture is a hard failure. It never degrades to a smaller corpus or
a synthetic substitute.** Silently substituting generated data is how
`fork_index_recall_bench.rs` came to report recall@10 = 1.000 while the index
under test never ran, and a fixture layer is a rich new home for that same bug.

A verified fetch prints a positive marker:

```
[fixture] OK name=beir-scifact-corpus rev=b3b5335604bf bytes=4469916 sha256=243324b35f03d82b path=/…
```

CI should assert on **that line**, not on the absence of a failure — a step that
silently does nothing prints no marker and so cannot pass a grep for one.

Benchmarks resolve a fixture by shelling out once at startup:

```rust
// The fetcher is the single implementation of the digest logic. Resolving
// through it means there is no Rust twin to drift from it, and no `sha2`
// dependency.
fn fetch_py() -> PathBuf {
    // Cargo runs benches with CWD = the *package* root (`crates/uni`), not the
    // workspace root, so a path relative to CWD would miss. Same trap the
    // rationale at `crates/uni/tests/common/bge_m3_real_onnx.rs` records for
    // the model cache.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/fixtures/fetch.py")
}

fn fixture(name: &str) -> PathBuf {
    let out = Command::new("python3")
        .arg(fetch_py())
        .args(["--print-path", "--only", name])
        .output()
        .expect("run scripts/fixtures/fetch.py");
    assert!(
        out.status.success(),
        "fixture {name} unavailable:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );
    PathBuf::from(String::from_utf8_lossy(&out.stdout).trim())
}
```

This is documented rather than shipped as `crates/uni/benches/common/fixture.rs`
on purpose: no bench consumes a fixture yet, and Cargo compiles only files that
are declared bench targets, so the module would sit in the tree **uncompiled by
anything** — the inert-code shape this track keeps finding. It lands with its
first real caller in the ANN work. The snippet above was compiled and run
against a warm and an empty cache before being written down.

`--print-path` verifies before it prints, so a path that comes back is a path
that matched its pin.

Every recall or latency line a fixture-backed benchmark emits must carry the
corpus identity and the **actual** N used — `corpus=beir-scifact n=5183 …`.
Today `dense_retrieval.rs` prints `docs={n}` with no corpus identity, which is
how a 1k synthetic run and a 1M real run become indistinguishable in a log.

## Scope

Present: BEIR SciFact (corpus, queries, qrels) — the fixture the layer was
proven against. Two repos and both digest algorithms in one fixture.

Not yet: SIFT-1M / GloVe / GIST (arrive with the ANN curves), LDBC SF1 (arrives
with the LDBC loader, and needs its `produced_by` generator invocation
recorded). Retry-with-resume, a `--gc` for unpinned revisions, and an
`actions/cache` layer are all deferred to the first fixture large enough to need
them — SciFact at 4.5 MB does not.

Retrofitting the ORT tarball and `website/scripts/prepare_*.py` onto this layer
is worthwhile and deliberately out of scope; doing it here would turn a
prerequisite into a cross-cutting refactor.
