# Documentation synchronization review — 2026-10-27

This review targets the current **5.0.0 source**, rather than assuming that
previously published wheels or historical release notes have this API. It
covers the website, `docs/UNI_BLACK_BOOK.md`, and the maintained agent skill
at `skills/uni-db/` (entrypoint and all twelve references).

## Verified discrepancies corrected

| Area | Incorrect documentation | Current source contract and correction |
|---|---|---|
| Concurrent transactions | Only one transaction could remain open per database; leaving one open leaked a global write lock | Transactions stage changes in private L0 buffers and can execute concurrently. Commits serialize and default SSI/OCC validates conflicts. The session's write-context restriction is distinct from a database-wide restriction. |
| MERGE | `ext_id` was mandatory and other MERGE patterns always created duplicates | Ordinary pattern properties match existing nodes. `ext_id` is optional and globally unique across labels, rather than unique per label. |
| Durability | Flush made uncommitted writes durable; L1 flush controlled query visibility | With WAL enabled, commit persists WAL before publishing mutations. Committed L0 data is query-visible before L1 flush. Transaction-local uncommitted writes are not made durable by `db.flush()`. Bulk loading has separate rollback limitations. |
| WAL internals | CRC-framed records; raw JSON only; incomplete mutation inventory and fictional recovery code | V2 is `UNIWAL2`, a BLAKE3 checksum, and a JSON segment payload. Segments also record writer features, announcement marks and commit spans. Legacy raw JSON remains readable. `SetVertexLabels` is a real mutation variant. |
| Identity | Packed label/type IDs, two-argument `Vid::new`, 44-character UIDs, wrong multibase prefix, arbitrary JSON hashing | Persisted IDs are dense and labels are resolved separately. A SHA3-256 UID is 53 base32 multibase characters with `b` prefix. The canonical content hashing helper is `VertexDataset::compute_vertex_uid`; UID-index hits can include fork entries. The stale Rustdoc length comment was corrected too. |
| Dependencies and versions | Black Book Lance 7; Rust reference advertised 2.0 | Source uses Lance 13.0.0, Arrow 58.4.1 and DataFusion 54.1.0. The API reference now identifies its 5.0.0 source target. |
| Database factories | Python and Rust factories claimed identical behavior; `in_memory()` claimed memory-only storage | Rust factories return builders; Python `Uni` factories return databases. Rust has no `Uni::builder()`. Both `in_memory()` implementations use temporary directories. Python builder configuration uses `UniBuilder`. |
| Python API and schema builders | Invented methods directly on Session; configuration chained after `Uni.open`; missing `.done()` transitions | Explain/profile/cursor terminals belong on query builders. Params belong on the Params facade. Python schema builders return to SchemaBuilder with `.done()` before switching label or edge type. Removed declarations and repaired examples. |
| Configuration | Five active fields absent from the website's full UniConfig declaration; skill omitted many fields | Both full Rust declarations now match all 45 source fields and types, including the deprecated no-op. Python accepts the 44 active fields; nested overrides, durations, cloud credentials and current Locy options are described separately. |
| Locy execution and defaults | SLG described as the current QUERY evaluator; goal-directed Off as default; synchronous Rust `derived_facts`; incorrect work-counter semantics | Auto is default; eligible bound goals use the fixpoint rewrite. Complete Rust rule facts are async/fallible via `derived_rules` / `derived_rule`; evaluated facts remain separately accessible. Python keeps `.derived` and `.derived_async()`. QUERY reads facts and does not increment `queries_executed`. |
| Rust notebook correctness | Removed public `derived` access, `derived().get()` on a mutation set, literal escaped newlines in raw programs, `DataType::Boolean` | Updated authoritative notebooks and their generators, regenerated Markdown, and compiled/executed all nine Rust Locy notebooks. Use `DataType::Bool`. |
| Fork API and scope | Python fork-local indexing said unavailable; Black Book listed shipped lifecycle and diff/promote APIs as future work; plugins described as always globally shared | Index construction exists on Python sync/async sessions. Lifecycle and diff/promote bindings are shipped. Instance plugins are shared; session registrations are local and shadow instance entries. Fork-local property extension remains limited; primary property additions widen live branches. Fork compaction remains a documented limitation. |
| Model providers | Rust defaults described as minimal; standard wheels advertised eleven providers | Rust defaults enable the provider set, including `remote/llamacpp`; standard wheels inherit twelve providers. Slim ONNX wheels retain ONNX plus eight hosted APIs. Smaller Rust builds require disabling defaults and selecting features. Python has no `Xervo.rerank` method; search procedures expose reranking options. |
| Agent routing | Fork reference existed but was omitted from the loading table | Added the fork reference route, corrected the algorithm count, and aligned the overview and language-specific references. The historical `CandleLinearClassifier` name is retained, but loading/inference no longer requires Candle. |

Implementation evidence includes `crates/uni/src/api/{mod,session,transaction,locy_result,xervo}.rs`,
`crates/uni-common/src/{config.rs,core/id.rs}`, `crates/uni-store/src/runtime/{writer,wal}.rs`,
`crates/uni-store/src/storage/vertex.rs`, `crates/uni-locy/src/{config,result}.rs`,
`crates/uni/Cargo.toml`, and the canonical Python stub and binding implementations.

## Drift prevention

`scripts/ci/check_doc_symbols.py` now covers the Black Book and notebook sources
in addition to website pages and the maintained skill. It excludes historical
skill evaluation workspaces. Besides global method names, it checks explicit
Python imports/factory return types, call arity, curated class declarations,
complete UniConfig field/type inventories, and known removed Rust Locy accesses
or malformed raw programs. Six negative controls run through the existing
static-guards CI entrypoint.

`scripts/verify_rust_locy_notebooks.py` combines the nine Rust Locy notebooks'
code cells into a temporary Cargo example, compiles it, and removes the temporary
source even on failure. `--run` executes them using temporary databases and mock
classifiers. This supplements the site converter, which cannot execute Rust
notebooks without an evcxr kernel.

## Validation

- Strict Zensical site build: **passed, no issues**.
- Notebook conversion: **39 converted, zero failures**; conversion did not execute Python notebooks or add captured outputs.
- Documentation contract checks: **212 documents, 752 Python blocks, 538 Rust blocks**.
- `python3 scripts/ci/static_guards.py --with-fuzz-lock`: **11 checks passed**, including all six documentation negative controls.
- All **nine Rust Locy notebooks compiled and executed successfully** in one temporary Cargo example. The reusable verifier's compile mode also passed.
- Five Python runtime probes passed: ordinary-property MERGE idempotence, two simultaneously open transactions committing, current Locy result access, configuration defaults/duration readback, and cross-label ext_id uniqueness.
- Agent skill validator: passed. Ruff checks/formatting for the new/expanded Python verification tools passed (the existing non-executable-script shebang convention is excluded from Ruff's EXE001 rule). Workspace Rustfmt check passed.
- Test/build processes ran in the foreground at normal scheduling priority.

Logs and the runtime probe/harness are preserved in the adjacent evidence
folder. The original temporary Cargo example was removed. No Git operations,
commits, pushes or GitHub writes were performed for this review.

## Scope and limits

The automated block counts measure inspected inputs, not fully executed examples.
Unknown Python receiver types and partial snippets are intentionally not treated
as proof of correctness. Loader guest snippets use a separate registrar API and
are excluded from Python receiver checks, but included in the site build. External model providers, cloud credentials, GPU paths,
and dataset-dependent Python flagship notebooks were not executed in this review.
Existing API/config parity guards and current source were used alongside targeted
runtime checks; a site build alone does not establish semantic correctness.
Historical release notes, dated proposals, and past skill evaluation outputs were
preserved as records rather than rewritten into current claims.
