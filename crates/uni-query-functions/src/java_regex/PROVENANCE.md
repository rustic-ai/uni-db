# Provenance of `java_regex`

This module is a clean-room implementation of Java 21 `java.util.regex`
matching (`Pattern.compile(p).matcher(s).matches()`) for Cypher's `=~`.
It was written from scratch in a sanitized copy of the repository from
which the previous implementation had been removed. This file records
what was consulted.

## Sources used

* **The behaviour specification** `docs/specs/java_regex_behaviour.md`,
  itself written from public documentation and black-box probes. Rule ids
  (`LB-3`, `CAP-5`, …) in the code refer to it.
* **Public JDK documentation**, as quoted by the specification (the JDK 21
  Javadoc of `Pattern`, `Matcher`, `PatternSyntaxException`, `Character`,
  `Character.UnicodeBlock`, `Character.UnicodeScript`).
* **Unicode 15.0 data and standards** from
  `https://www.unicode.org/Public/15.0.0/ucd/`: `PropList.txt`,
  `auxiliary/GraphemeBreakProperty.txt`, `auxiliary/GraphemeBreakTest.txt`
  (copied to `testdata/GraphemeBreakTest-15.0.0.txt`), `emoji/emoji-data.txt`,
  `UnicodeData.txt`; UAX #29 for the grapheme-cluster rules, UAX #15 /
  UAX #44 for canonical decomposition and composition.
* **Generated data**:
  * `ucd_tables.rs`, `ucd_names.bin`: already in the repository, produced
    by `scripts/java_regex_oracle/GenUcdTables.java` from JDK 21's
    `Character`, `Normalizer` and `Pattern` (used unchanged);
  * `ucd_extra.rs`: generated for this implementation by
    `scripts/java_regex_oracle/gen_ucd_extra.py` from `PropList.txt`,
    `GraphemeBreakProperty.txt` and the output of
    `scripts/java_regex_oracle/ScanGraphemeClasses.java`, a black-box scan
    of how JDK 21's `\X` treats every code point.
* **Black-box experiments on the installed runtimes** (Eclipse Temurin
  21.0.9+10 and OpenJDK 25.0.4.1), through the public API only:
  * `scripts/java_regex_oracle/JavaRegexProbe.java` (matches, groups, find
    positions, compile errors for hand-written probe lists);
  * the existing oracle `scripts/java_regex_oracle/JavaRegexOracle.java`
    with cases from `gen_cases.py` (all twelve profiles, two seeds), and the
    curated `testdata/java_regex_oracle.tsv`;
  * small one-off programs in the clean-room work directory (not part of
    the repository): `ScanCanon.java` (which code points extend a `(?c)`
    class match), `SliceCase.java` (case-insensitive literal runs against
    single literals over all case-relevant pairs), `ClassSet.java` (the
    sets of 29 341 generated bracket classes over a small alphabet), and a
    timing harness.
* **Generic regex-engine background**: backtracking matchers with explicit
  stacks, and the public API documentation of the `regex-automata` and
  `regex-syntax` crates (used for the linear route).

## Not consulted

OpenJDK source in any form, decompiled classes, any existing
implementation of Java regex semantics (including the module this one
replaces and its design notes), and anything outside the clean-room
directory other than the JDK runtimes executed as black boxes.

## Behaviour established by probes beyond the specification

Each item was found by a gate disagreement or a targeted probe and then
checked over many generated cases; the code comments repeat the evidence.

1. **Loop strategies.** Java matches each iteration of a quantified single
   atom, of a quantified group whose body can match only one way, and of
   any possessive quantifier atomically (first match only), giving whole
   iterations back on failure; other group loops explore every way. This
   explains `\R` atomicity under quantifiers (`(?:\R)+\n`, `(?:a\R)+\n`
   fail on `\r\n`, `(?:\R)?\n` and `(?:\R|x)+\n` match), the CAP-5 capture
   quirk, and that `((b*)b){2}+a` fails on `bba`. An empty iteration below
   the minimum counts and the loop continues (`(a)(\b{g}){2,3}a` fails on
   `aa`). An empty iteration past the minimum drops only the capture of the
   group the loop repeats (CAP-5); groups inside it keep theirs:
   `(?:())*\1` matches the empty string and `((\b))*a\2` matches `a`,
   while `(\b)*a\1` and `((\b))*a\1` do not.
2. **`\b{g}`** consults a hidden position `last`: at an inner position it
   holds iff the position is at or past the end of the grapheme cluster
   starting at `last`; it never holds inside a surrogate pair. `last` is
   moved by look-ahead bodies (positive and negative), atomic groups, and
   iterations of atomic-iteration loops except greedy `x*`/`x+` on one
   code-point matcher; look-behind bodies and `\X` do not move it. When
   `last` equals the input length Java throws
   `StringIndexOutOfBoundsException`.
3. **`(?c)`** affects bracket classes and `\p{…}` only: they match a
   prefix of two or more code points of the grapheme cluster at the
   position whose NFC is one code point in the class (longest first, shorter
   ones on backtracking), or the cluster's only code point.
4. **`\Q…\E` is removed before parsing**: quoted ASCII letters and digits
   and non-ASCII characters are copied as they are, except that a digit
   right after `\Q` becomes `\x3d`, and other ASCII characters are escaped:
   `\u004\Qa` is `J`, `a*\Q\E+` is `a*+`, `[\Q\E]` is unclosed; error
   indices count positions in the result (`\Q1\E(` reports index 5,
   `\Q12\E(` 6, `\Qé\E(` 2) and `\N{w00\Q4}` names `w00\x34\`.
5. **Bracket classes**: single characters are kept in one set that the
   class's intersections refer to as it is at the end; `&&` intersects with
   the most recent non-single member when nothing follows it; `[a&&[b]&c]`
   is {a, c, &}, `[[a][b]&&]` is {b}, `[[a]b&&]` is "Bad intersection
   syntax". Only characters whose case closure lies below U+0100 are
   collected this way; any other is a member of its own (`[😀a&&]`,
   `[\x{100}a&&]` and `(?iu)[ka&&]` are refused like `[[a]b&&]`, while
   `[a😀&&]` and `[\p{L}😀&&]` compile; 271 048 random classes over such
   characters agree with JDK 21). `&` and `-` follow from Java's
   range parsing (`[&--]` is {&, -}); `\v` before `-` or as a range end is
   U+000B.
6. **Literal runs**: two or more adjacent literals compare
   case-insensitively without the guard of CI-2 (`(?iu)aß` matches `aẞ`,
   `(?iu)ß` does not match `ẞ`).
7. **COMMENTS mode**: a comment ends before any line terminator of the
   current mode (`(?x)a#c b` matches `a b`); the character after
   `(?` is read raw.
8. **Look-behind lengths**: the acceptance check and the window both use
   Java's 32-bit arithmetic as described in `analyze.rs` (wrapping sums,
   a "running value must not decrease" check on counted quantifiers, the
   check restarting after a branch, a branch at least −1 wide, `\X` and
   `(?c)` classes counting as minimum 1 / maximum 0). When a supplementary
   character appears in the pattern at or after the look-behind, the
   window is counted in code points (starts step back one code point at a
   time, a negative maximum meaning none, and with fewer code points before
   the position than the minimum the body still runs from the input start:
   `(?<!(𐐨?)++T)\1` matches the empty string, the capture leaking out of the
   failed body), otherwise in UTF-16 units; this refines the spec's K rule
   (LB-2). A plain or capturing group repeated by a greedy or lazy counted
   quantifier other than `?` must match one way by the loop-strategy test of
   item 1, atomic groups inside included, even with a count of zero
   (`(?<!((?>a?)){0})` is refused, `(?<!(?>a?){0})` compiles). A stress fuzz of 102 586 look-behind
   cases with counts near 2^31 leaves 22 disagreements, all with
   `{2147483647,}` or ~10^9-iteration zero-width loops (Java's arithmetic
   there is not fully characterized).
9. **Properties**: with `U`, POSIX names other than `ASCII` are found
   case-insensitively; `\p{L1}` is Latin-1; `gc=` takes the same names as
   a bare property; `Is…`, script and block names are compared after
   `toUpperCase(Locale.ROOT)`, keys after `toLowerCase`; `\N{…}` trims
   characters up to U+0020 and upper-cases.
10. **Grapheme data**: JDK 21's `\X` treats unassigned code points as
    Control except reserved Extended_Pictographic ones and U+0378, and
    U+11720..U+11721 as spacing marks.
11. **Error indices and order** (ERR-3 left them open). An index counts
    code points of the pattern after `\Q…\E` removal. Most errors found
    after reading a construct point at its last character (`a{3,2}` at 5,
    `\p{Foo}` at 6, `[z-a]` at 3); "Illegal repetition" points one past
    the `{`; a count past `Integer.MAX_VALUE` or a `\x{…}` past U+10FFFF is
    refused at the digit that makes it so; "Dangling meta character" points
    just before what follows, COMMENTS-mode whitespace skipped. A
    look-behind's length is checked as soon as its body ends, before its
    `)` and anything after it, and reported just before the `)` (or the
    end). Descriptions: `(?$` and `(?@` are "Unknown group type"; `\x{`
    without a first digit is "Illegal hexadecimal escape sequence"; `\b{g`
    without `}` and `[a-\p…` are "Illegal/unsupported escape sequence";
    `(?x)\c` followed only by whitespace or a comment is "Unexpected
    internal error"; a trailing `\` escapes the end of the pattern (`(\` is
    an unclosed group at 3) and is reported as such only at the top level.
    In COMMENTS mode a `#` comment can hide the `}` of `\N{…}` and
    `\p{…}`, whose names keep their spaces and comments. Checked against
    JDK 21 on 812 000 cases: every refused pattern of the gate corpora
    (97 309), of the cross-check corpus (44 043), and 671 000 random
    mutations of gate patterns.

