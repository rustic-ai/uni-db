# Rust and Python public API parity review

The review compares the current Rust public database/session/transaction
facades, builders, returned data, operational configuration and cloud storage
configuration with the Python sync/async wrappers, stubs and documentation.
This is an implementation review and targeted validation record, not a claim
that every native Rust type has an identical Python representation.

## Verified gaps and repairs

| Finding | Result |
|---|---|
| Python accepted only 19 of 45 `UniConfig` fields | All 44 active fields are accepted and returned by `db.config()`; only the deprecated `partial_lance_writes` no-op remains excluded |
| File-sandbox, object-store request policy and index-rebuild configs were not exposed | All fields of these nested structs are bound; existing compaction/throttle dictionaries retain merging |
| Readback contained keys that could not be passed to `config()`; fork durations were returned as seconds but accepted only timedeltas | Readback uses exactly the accepted keys; durations accept seconds or timedelta, with `None` for optional values |
| Builder `.config()` took the saved config before conversion; an error lost preceding settings | Convert a clone, and replace the saved config only after success |
| Cache and parallelism setters always overrode config dictionaries, irrespective of call order | All setters use the same saved config; the last setter wins |
| Negative, non-finite or overflowing config durations could panic through Rust's `from_secs_f64` | Checked duration conversion raises Python `ValueError` |
| Locy semiring, neural feature opt-out and classifier-cache capacity were absent | Available in both `LocyConfig(...)` and the dictionary configuration form; object properties expose the settings |
| Unknown Locy configuration keys were silently ignored | Refuse unknown keys with accepted-key diagnostics |
| Rust cloud credentials accepted optional `None`, but Python's string extraction rejected it; unknown keys were ignored | Preserve optional credential fields for all three providers; reject unknown provider-specific keys |
| Session APIs and transaction builder conveniences were missing | Sync/async bindings for config-aware Locy compilation, manual fork-local index building, default retry execution and replacement-scan controls; bulk `.params()` on execute/Locy transaction builders |
| Database-wide Python source-plugin loading was absent despite the Rust adapter | Sync/async `db.load_python_plugin` registers in the database registry; independent sessions can invoke it; session loading remains session-scoped |
| Background task count was Rust-only | Both Python database flavors expose it |

Compilation through `compile_locy_with_config` deliberately follows Rust's
configuration-taking API. The existing `compile_locy` entry point retains its
Rust behavior of disabling neural declarations, even though the evaluation
config defaults to enabling them. No Rust defaults or isolation semantics were
changed to satisfy Python tests.

## Continuing parity checks

The source guard now checks 19 facade/builder/result types (rather than only
eight builder/result types), 12 returned data types (rather than four), both
Locy configuration entry points, all five nested database config structs and
all cloud-storage variants. It compares config acceptance with input application
and output keys. Negative controls add an unbound method, a top-level field,
a nested field or a cloud credential/provider, or remove a readback key, and
verify the guard fails. Runtime sync/async parity, stub reflection, and stubtest
remain independent checks against the compiled extension.

The [inventory](python_api_parity_2026_10_27/api-inventory.json) records the exact
covered methods, fields and deliberate differences:

- Constructors, ownership transfers and Rust iterator adapters use Python's
  existing factories, properties and iteration conventions.
- Native plugin registry/trait handles, scheduler callback/job-record APIs,
  service-embedder principal objects and storage internals do not have Python
  equivalents. Python callbacks use the existing retry helpers and plugin
  loaders. These exclusions have explicit reasons in the guard; they are not
  silently counted as bindings.
- The Rust linear classifier and shared internal classifier cache/provenance
  handles are not exported as Python native objects. Python registers classifier
  callables; the binding manages the provenance store. Cache capacity is now
  configurable.
- Python exposes the complete Locy derived-fact view through `derived`,
  `derived_async` and `derived_facts`; it does not expose native lazy-materializer
  handles or intermediate-only views.
- Existing exception coverage is unchanged: 56 of 67 Rust variants have dedicated
  Python exceptions. The remaining variants use existing base exceptions or
  originate from native-only APIs; the inventory names each one and its reason.

The exclusions mean source coverage is reviewed and explicit, not that the two
languages have byte-for-byte identical APIs.

## Validation

The final rebuilt extension passed **1,189 Python tests**, with 15 existing skips
(178.82 seconds). This includes the new config/API regressions, sync/async method
parity and runtime/stub reflection checks. Stubtest reported no issues in six
modules. Strict Clippy passed for every target of `uni-python`; the slim
`uni-python-onnx` binding also compiled successfully. Ruff passed for the binding
package, all 115 Python/stub files were formatted, and Rust formatting passed.
All source guards and the seven negative controls passed; the final source-guard
run also checked the fuzz lockfile.

See the [validation manifest](python_api_parity_2026_10_27/validation.json) for
commands, saved logs and source hashes. GPU wheel builds and live cloud-service
connections are not part of these results.

Python async tests require host-level execution in this environment: the sandbox
also stalled an unchanged `AsyncUni.temporary()` baseline, while the same module
opened and closed immediately with host-level access. Tests and builds run in the
foreground at normal scheduling priority. GPU wheel builds and live cloud-service
connections are outside this review; cloud config tests validate conversion
without contacting providers.
