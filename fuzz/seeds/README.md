# Fuzz seed corpus

Regression seeds for inputs that previously crashed. `cargo fuzz run`
merges these into the working corpus:

```bash
cargo +nightly fuzz run btic_decode corpus/btic_decode seeds/btic_decode
```

**Name `corpus/` first — the order matters.** libFuzzer writes every
newly-discovered input into the *first* corpus directory on the command line and
treats the remaining ones as read-only. The shorter form
`cargo fuzz run btic_decode seeds/btic_decode` therefore makes *this*
directory the output corpus, and one 30-second run buries its handful of
curated regression inputs under several hundred generated files. (Measured:
1 file became 384.) These are git-tracked, so that lands in `git status` as
hundreds of untracked files rather than anything louder.

- `btic_decode/utf8-boundary-bce-suffix` — multi-byte UTF-8 straddling the
  `len - 3` byte index panicked `strip_bce_suffix` (fixed 2026-06-10).
