# Java 21 `java.util.regex` behaviour specification (clean-room)

Status: specification for a clean-room reimplementation. Written from the public Javadoc,
the Unicode Standard / UCD 15.0, and black-box experiments against JDK 21 (Temurin
21.0.9+10) and JDK 25 (OpenJDK 25.0.4.1). No JDK source code, decompiled code, or
existing implementation was consulted. Every behavioural statement below is either a
quotation of public documentation (attributed) or a summary of probe results (with
probe ids). Statements that were inferred but not probed are marked **unverified**.

## 0. Scope and conventions

### 0.1 What uni-db needs

Cypher `s =~ p` is evaluated as `Pattern.compile(p).matcher(s).matches()` with Java 21
semantics: the pattern is compiled **with no flags**, and the whole input must match.
The only way a Cypher user can change flags is with inline flag groups
`(?idmsuxU-idmsuxU)` / `(?idmsuxU-idmsuxU:X)`. Consequently:

* Flags `CANON_EQ` and `LITERAL` are unreachable from Cypher (there is no inline letter
  that enables them; see FLG-6). They are documented briefly in §11 for completeness only.
* `find()`, `lookingAt()`, regions, `hitEnd()` are out of scope, except where a probe
  used `find()` to *observe* zero-width positions (marked "find" in examples).
* Results that matter: (a) does compilation throw `PatternSyntaxException` (and with
  which description / index), (b) the boolean result of `matches()`, (c) whether a
  `StackOverflowError` occurs. Capture-group contents matter only through
  back-references, but they are recorded where they explain a rule.

### 0.2 Text model

Java strings are sequences of UTF-16 code units. Many rules below are stated in terms of
UTF-16 *unit indices*; an implementation working on UTF-8 must still model the UTF-16
view to reproduce them (lookbehind start positions, `\b` look-back, back-reference
comparison, lone-surrogate literals). A host whose strings are always valid Unicode
(e.g. Rust `String`) never presents unpaired surrogates in the *input*, but the
*pattern* may still contain `\uD800`-style escapes, and match positions can still fall
inside a surrogate pair (LB-7, BR-8).

"Reads the code point at index i" means: if unit i is a high surrogate and unit i+1 is a
low surrogate, the code point is the pair and spans 2 units; otherwise the code point is
unit i alone (a lone surrogate, 1 unit). Pairing is only ever looked for *forward* from
i. Rule SUR-1 makes this precise.

### 0.3 Unicode version

JDK 21 implements Unicode 15.0 (Javadoc of `java.lang.Character`: "Character
information is based on the Unicode Standard, version 15.0"). JDK 25 implements Unicode
16.0. All property sets, case mappings and grapheme rules in this document are for 15.0
unless stated.

### 0.4 Notation

* Examples are written `pattern` ⟶ input ⟶ result. Patterns are shown as the *regex
  text* (not as a Java string literal): `\d` means backslash-d. Inputs are shown with
  `\uXXXX` / `\x{X}` notation for non-ASCII. `MATCH`/`NOMATCH` are `matches()` results;
  `PSE "desc" @i` is a `PatternSyntaxException` with `getDescription()` = desc and
  `getIndex()` = i.
* Flags in examples: `i` CASE_INSENSITIVE, `u` UNICODE_CASE, `m` MULTILINE,
  `s` DOTALL, `x` COMMENTS, `d` UNIX_LINES, `U` UNICODE_CHARACTER_CLASS. Unless
  stated, results are identical when the flag is given inline (`(?i)` etc.).
* Probe ids (e.g. `B-neg-inter4`) refer to the probe log (Appendix B). All probes were
  run on both JDK 21 and JDK 25; results are identical unless a "JDK 25" note says
  otherwise.
* U(c), L(c), T(c): simple uppercase / lowercase / titlecase mapping of code point c
  (UnicodeData.txt fields 12, 13, 14; identity if empty; T defaults to U). Verified:
  Java's `Character.toUpperCase/toLowerCase/toTitleCase(int)` equal these for every
  code point (2 879 mapped code points, 0 differences; `MapDump`).

---

## 1. Errors

**ERR-1 (exception type).** Every syntax error is reported as
`java.util.regex.PatternSyntaxException` (an `IllegalArgumentException`). Compile-time
stack exhaustion is also converted into a `PatternSyntaxException` with description
`Stack overflow during pattern compilation` (RES-3). Match-time stack exhaustion is a
`java.lang.StackOverflowError` thrown from `matches()` (RES-1).

**ERR-2 (message format).** `getMessage()` is
`description` + (index ≥ 0 ? `" near index " + index` : "") + line separator + pattern +
(0 ≤ index < pattern length ? line separator + index spaces + `^` : ""). Examples
(probe `Msg`): `)` → `Unmatched closing ')'` (index −1, no "near", no caret);
`a)` → `Unmatched closing ')' near index 0` + caret under `a`; `(` → index 1, no caret
line. Javadoc (PatternSyntaxException.getMessage): "Returns a multi-line string
containing the description of the syntax error and its index, the erroneous
regular-expression pattern, and a visual indication of the error index within the
pattern."

**ERR-3 (descriptions).** Appendix C lists every description string observed with the
construct that produced it. The index conventions are irregular (often "one past the
offending text", sometimes the start); they are recorded per probe in Appendix C. An
implementation that wants message parity should copy descriptions verbatim; index
parity is best-effort.

---

## 2. Literals and escapes

**LIT-1 (ordinary characters).** Any character other than the metacharacters
`\ [ ( ) | ^ $ . ? * + {` is a literal outside classes. `]` and `}` alone are literals
(`]` ⟶ `]` MATCH, `}` ⟶ `}` MATCH; Q-lit-close2/3). `{` is a literal only where it
cannot start a counted quantifier — in practice almost never: `a{` and `a{x}` are
`PSE "Illegal repetition"` (QNT-1).

**LIT-2 (backslash + non-alphanumeric).** A backslash before any character that is not
an ASCII letter or ASCII digit quotes it: `\!` `\"` `\#` `\-` `\ ` `\]` `\}` `\_` etc.
all match the character (A-nonal-*). This includes non-ASCII characters, even letters
and supplementary characters: `\é` ⟶ `é`, `\一` ⟶ `一`, `\😀` ⟶ `😀` (A-bs-nonascii*,
A-bs-supp). Javadoc: "It is an error to use a backslash prior to any alphabetic
character that does not denote an escaped construct … A backslash may be used prior to
a non-alphabetic character regardless of whether that character is part of an
unescaped construct." (Observed: "alphabetic" means ASCII letters only.)

**LIT-3 (backslash + ASCII letter).** Valid outside classes:
`\a \e \f \n \r \t` (controls 07, 1B, 0C, 0A, 0D, 09), `\c`, `\x`, `\u`, `\N`, `\Q`,
`\d \D \h \H \s \S \v \V \w \W`, `\p \P`, `\b \B \A \G \z \Z`, `\R`, `\X`, `\k`.
`\E` is valid only as the terminator of `\Q` (a stray `\E` is
`PSE "Illegal/unsupported escape sequence"`, A-Q-E-alone). Every other ASCII letter
(`g i j l m o q y C F I J K L M O T U Y`, and `E` when stray) is
`PSE "Illegal/unsupported escape sequence"` with index = position of the letter
(A-let-*). Specific errors: `\c` at end → `Illegal control escape sequence`; `\k`
without `<` → `\k is not followed by '<' for named capturing group`; `\p`/`\P` at end
→ `Unknown character property name {\u0000}` (sic, a NUL in braces);
`\u` without 4 hex digits → `Illegal Unicode escape sequence`; `\x` without hex →
`Illegal hexadecimal escape sequence`; `\N` not followed by `{` → `Illegal character
name escape sequence`.

**LIT-4 (backslash + digit).** `\0` introduces an octal escape (LIT-5). `\1`–`\9`
introduce back-references (BR-1). A trailing lone `\` is `PSE "Unescaped trailing
backslash"` (A-trailing-bs).

**LIT-5 (octal).** `\0` must be followed by 1–3 octal digits; three digits are taken
only if the first is 0–3. `\07`=07, `\077`=3F, `\0377`=FF; `\0400` = `\040` followed by
literal `0` (⟶ `" 0"` MATCH); `\03777` = FF + `7`. `\0` alone, `\0` at end, or `\08` →
`PSE "Illegal octal escape sequence"` (A-oct-*).

**LIT-6 (hex).** `\xhh`: exactly two hex digits (either case). `\x{h…h}`: one or more hex
digits, leading zeros unlimited (`\x{00000000000000000041}` compiles), value ≤ 10FFFF;
`\x{110000}` → `Hexadecimal codepoint is too big`; `\x{}` → `Illegal hexadecimal escape
sequence`; `\x{41` (no `}`) or a non-hex char inside braces → `Unclosed hexadecimal
escape sequence` (A-hex*).

**LIT-7 (`\u`).** `\uhhhh`: exactly four hex digits; `\uu0041` is an error (unlike the
Java-language lexer). Two consecutive `\u` escapes forming a high+low surrogate pair
denote one supplementary code point (`😀` ⟶ `😀` MATCH, and quantifiers apply
to the whole pair: `😀+` ⟶ `😀😀` MATCH; A-u-pair*), also inside classes
(`[😀]` is a one-element class containing U+1F600, A-u-pair-class).
Pairs are **not** formed across other escape kinds or with literal surrogates:
`\x{D83D}\x{DE00}`, `\uD83D\x{DE00}`, `\x{D83D}\uDE00`, literal-high + `\uDE00`,
`\uD83D` + literal-low all denote two lone surrogates and do not match `😀`
(A-hexbr-surr, A-u-hi-then-x-lo, A-x-hi-then-u-lo, A-lit-hi-then-u-lo,
A-u-hi-then-lit-lo).

**LIT-8 (`\N{name}`).** Resolves `name` exactly as the public method
`Character.codePointOf(name)`: Javadoc: "The name matching is case insensitive, with
any leading and trailing whitespace character removed. For the code points in the
UnicodeData file, this method recognizes the name which conforms to the name defined
in the 'Unicode Name Property' section in the Unicode Standard. For other code points,
this method recognizes the name generated with getName(int) method", where the generated
name is "`Character.UnicodeBlock.of(codePoint).toString().replace('_', ' ') + " " +
Integer.toHexString(codePoint).toUpperCase(Locale.ROOT)`". Observed (A-N-*):
`\N{white smiling face}` ⟶ U+263A; `\N{ LATIN SMALL LETTER A }` OK; tab trimmed;
`\N{CJK UNIFIED IDEOGRAPHS 4E00}` OK but the Unicode-standard name
`CJK UNIFIED IDEOGRAPH-4E00` is rejected; `HANGUL SYLLABLES AC00` OK but
`HANGUL SYLLABLE GA` rejected; `PRIVATE USE AREA E000`, `HIGH SURROGATES D800` OK;
unassigned code points have no name (`GREEK AND COPTIC 378` rejected). Name aliases
(`LINE FEED`), `CONTROL-000A`, underscores, internal double spaces, loose matching are
all rejected. Errors: unknown → `Unknown character name [name]` (index after `}`);
missing `}` → `Unclosed character name escape sequence`; `\N{}` → `Unknown character
name []`. The result is a single code point and may be supplementary
(`\N{GRINNING FACE}` ⟶ `😀`). Works inside classes and as range endpoints.
Implementation note: control characters' names in UnicodeData are `<control>`, so the
only accepted form for them is the Java Unicode-1-style name (`\N{NULL}` ⟶ U+0000 works,
A-N-ctrl). **Unverified:** the full mapping for U+0000–U+001F/U+007F–U+009F; an
implementer should generate the name table from the JDK 21 public API
(`Character.getName` over all code points) as black-box data.

**LIT-9 (`\cX`).** `\c` takes the next pattern character X (even `\` — `\c\` is
U+001C, and `\c\\` leaves a trailing `\` error, A-c-bs*) and denotes X XOR 0x40:
`\c@`=00, `\cA`=01, `\ca`=21 (`!`), `\c?`=7F, `\c1`=71, `\c `=60, `\cé`=A9 (A-c-*).
**Unverified** for supplementary X (`\c😀` matched nothing in probe A-c-supp-f).

**LIT-10 (`\Q…\E`).** Everything between `\Q` and the next `\E` is literal (including
`\`; `\Q\\E` is one backslash, A-Q-bs). Missing `\E` quotes to end of pattern
(A-Q-noE). `\Q\Q` quotes the second `\Q` (A-Q-nested). Inside a class, quoted text adds
its characters to the class and suppresses range syntax: `[\Qa-c\E]` = {a,-,c}
(A-Q-class2/3); `[\Qab]` (no `\E`) swallows `]` → `Unclosed character class`
(A-Q-classnoE). A quantifier after `\Q…\E` applies to the last quoted character
(`\Qab\E+` ⟶ `abb` MATCH). An empty `\Q\E` produces nothing: `a\Q\E+` = `a+`
(A-Q-empty), `x\Q\E{2}` = `x{2}` (Q4-Q-quant2); at pattern start `\Q\E*` is
`Dangling meta character '*'` @0 and `\Q\E{2}` matches the empty string (QNT-5).
Case-insensitivity applies to quoted text (A-Q-ci). In COMMENTS mode whitespace and `#`
inside `\Q…\E` are literal (A-Q-x, X-hash-in-Q).

---

## 3. Character classes `[...]`

**CLS-1 (opening).** `[` opens a class; `[^` a negated class. A `]` immediately after
`[` or `[^` is a literal member (`[]a]` ⟶ `]` MATCH; `[^]a]` ⟶ `]` NOMATCH). Hence
`[]` and `[^]` are `Unclosed character class` (B-empty, B-neg-empty).

**CLS-2 (members).** Members are single characters (literal or escaped, including
`\Q…\E` content and `\N{}`), ranges, predefined classes (`\d \w \s \h \v` and
negations), properties (`\p{}`/`\P{}`), and nested classes `[...]`. A `[` anywhere
inside a class opens a nested class, so a literal `[` must be escaped (`[[]` →
`Unclosed character class` @2; `[\[]` ⟶ `[` MATCH). Metacharacters `. $ ^ * + ? ( ) { }
|` are literals inside a class (B-meta-inclass, B-dot-inclass). `^` not at the start is
literal (`[a^]`, `[^^]`).

**CLS-3 (escapes not allowed in classes).** `\b \B \A \G \z \Z \R \X \1`…`\9`,
`\k<…>` inside a class → `Illegal/unsupported escape sequence` @(index of the escape
letter) (B-esc-*, D-R-inclass, D-X-inclass, D-bg-inclass). In particular `[\b]` is an
error, not backspace.

**CLS-4 (ranges).** `x-y` where x and y are single characters (literals, escapes,
`\N{}`, one-character `\Q…\E` text) is the inclusive code-point range;
y < x → `Illegal character range` (B-range-rev). Ranges compare code points, so
`[\x{1F600}-\x{1F64F}]` contains U+1F610 and not the lone surrogate D83D
(B-range-supp*). A range may span BMP and supplementary (`[a-\x{1F64F}]` contains
U+1F000 and lone D800, B-range-bmp-supp*). `[\uD800-\uDFFF]` matches lone surrogates
but not a surrogate pair (B-range-surr*).

**CLS-5 (hyphen rules).** `-` is literal when first (`[-a]`), last (`[a-]`), right after
a completed range (`[a-c-e]` = {a,b,c,-,e}), after a predefined class or property
(`[\d-z]` = digits ∪ {-,z}; `[\p{L}-z]` = letters ∪ {-}), and when followed by a nested
class (`[a-[b]]` = {a,-,b}, B2-range-to-class). `-` as range *end* is an error
(`[a--]` → `Illegal character range`), but `-` as range *start* is fine
(`[--/]` = {-,.,/}). A range whose end is a predefined class/property (`[a-\d]`), or
`[a-` at end of pattern, → `Illegal character range`. An escaped `]` is not allowed as
a range end (`[a-\]]` → `Illegal character range`; B2-dash-esc-bracket). A range start
of `]` is fine (`[]-a]` = `]`..`a`).

**CLS-6 (union).** Juxtaposition is union; nested classes union in
(`[a-d[m-p]]` ⟶ `n` MATCH, `[a[b[c]]]` ⟶ `c` MATCH).

**CLS-7 (intersection `&&`).** `&&` intersects the union on its left with the union on
its right; it has *lower* precedence than union (Javadoc precedence table: "4 Union …
5 Intersection"). `[a-c&&b-dx]` = [a-c] ∩ ([b-d] ∪ {x}) = {b,c} (x NOMATCH, a NOMATCH,
b MATCH; B-inter-union-prec*). `[xa-c&&b-d]`: x NOMATCH. Chains associate:
`[a-z&&b-y&&c-x]` = c..x. Right operand may be bare or bracketed (`[a-z&&def]` =
`[a-z&&[def]]`). Degenerate forms (B2-amp*):
* a single `&` is a literal (`[a&b-d]` = {a,&,b,c,d});
* empty operands are ignored: `[a&&]` = {a}; `[&&a]` = {a}; `[^&&a]` = complement of {a};
* runs of k ≥ 3 `&` between members (generalized from 9 probes, B2-amp3/3b/4, Z-amp-*):
  if k is **even** the run acts as intersection(s) with the empty middle operands
  ignored (`[a&&&&b]` = ∅, `[ab&&&&b]` = {b}); if k is **odd** no intersection happens
  and `&` is added as a literal member (`[a&&&b]` = {&,a,b}, `[ab&&&b]` = {&,a,b},
  `[a&&&&&b]` = {&,a,b}, `[a&&&]` = {&,a}); a later `&&` still intersects
  (`[a&&&b&&c]` = ∅). An odd run at the very start is an error (`[&&&a]` →
  `Bad class syntax` @2);
* `[&&]` alone → `Bad class syntax` @2;
* `[a&&[]]` → `Unclosed character class` (inner `[]` swallows `]`).

**CLS-8 (negation scope).** A leading `^` negates the **entire** class expression of
that bracket, including nested classes and intersections (B-neg-*):
`[^a[b]]` = ¬{a,b}; `[^[b]a]` = ¬{a,b}; `[^a[^b]]` = ¬({a} ∪ ¬{b}) = {b};
`[^a&&b]` = ¬({a}∩{b}) = everything; `[^a-z&&[aeiou]]` = ¬vowels;
`[^b&&[a-c]]` = ¬{b}. Nested `[^…]` negates only itself.

**CLS-9 (case-insensitive classes).** See CI-3/CI-4/CI-5. Negation is applied after
case closure: `(?i)[^K]` does not match `k` (B-ci-neg-dup2); `(?iu)[^k]` does not match
U+212A (CI-c3).

**CLS-10 (COMMENTS mode in classes).** With `x`, whitespace (XM-1) inside classes is
ignored and `#` starts a comment even inside a class: `(?x)[a b]` = {a,b};
`(?x)[a#b]\n]` = {a}; `(?x)[a - c]` = a..c; `(?x)[a& &b]` = `[a&&b]`;
`(?x)[^ a]`: whitespace before `^` makes `^` a literal (X-sp-cls-neg*). Escape it
(`[\ ]`, `[\#]`) to make it significant. (Javadoc COMMENTS: "Comments mode ignores
whitespace within a character class contained in a pattern string. Such whitespace must
be escaped in order to be considered significant.")

**CLS-11 (unclosed).** Missing `]` → `Unclosed character class`, index usually the last
index of the pattern (`[a` @1, `[[a]` @3, `[` @0, `[]` @1; but `[\Qab]` @4)
(B-unclosed*, B-only-open, A-Q-classnoE).

---

## 4. Predefined classes, `.` and properties

### 4.1 Predefined classes (all verified over all 1 114 112 code points, `PropCheck`)

**PRE-1 (ASCII mode, no `U`).** `\d`=[0-9]; `\s`=[ \t\n\x0B\f\r]; `\w`=[a-zA-Z_0-9];
`\h`=[ \t\xA0 ᠎ -   　] (19 cps);
`\v`=[\n\x0B\f\r\x85\u2028\u2029] (7 cps); uppercase forms are complements (and match
lone surrogates and supplementary characters, U-hi-05…09).

**PRE-2 (`U` mode).** With UNICODE_CHARACTER_CLASS: `\d` = gc Nd (680 cps);
`\s` = White_Space (PropList, 25 cps); `\w` = Alphabetic ∪ gc Mn ∪ gc Me ∪ gc Mc ∪ gc Nd ∪
gc Pc ∪ Join_Control (139 612 cps). `\h`, `\v` are unchanged by `U`. These match the
Javadoc table "predefined and posix character classes in Unicode mode".

**PRE-3 (`.`).** Without `s`: any code point except the line terminators
\n \r \u0085 \u2028 \u2029 (1 114 107 cps; \x0B and \f are matched). With `d` (and no
`s`): any code point except \n. With `s`: everything. `.` matches a lone surrogate as one
unit and a surrogate pair as one code point (U-hi-00, U-pair-00; `..` does not match
`😀`, U-pair-dot2). `.` never matches the two units of `\r\n` as one.

**PRE-4 (POSIX `\p{Name}`, ASCII mode).** Exactly as the Javadoc table:
Lower [a-z], Upper [A-Z], ASCII [\x00-\x7F], Alpha, Digit [0-9], Alnum,
Punct = the 32 ASCII punctuation/symbols ``!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~``,
Graph [\x21-\x7E], Print [\x20-\x7E], Blank [ \t], Cntrl [\x00-\x1F\x7F],
XDigit [0-9a-fA-F], Space [ \t\n\x0B\f\r]. Names are case-sensitive (`\p{lower}` →
error, P-161).

**PRE-5 (POSIX in `U` mode).** Lower = Lowercase; Upper = Uppercase; ASCII unchanged;
Alpha = Alphabetic; Digit = gc Nd; Alnum = Alphabetic ∪ Nd; Punct = gc P (Pc Pd Ps Pe Pi
Pf Po); Graph = ¬(White_Space ∪ Cc ∪ Cs ∪ Cn); Print = (Graph ∪ Blank) − Cc;
Blank = White_Space − {Zl, Zp, \n \x0B \f \r \x85}; Cntrl = Cc; XDigit = Nd ∪ Hex_Digit
(UCD); Space = White_Space. (All 14 verified exactly, `PropCheck base`.)

### 4.2 Property syntax and name resolution

**PROP-1 (forms).** `\p{name}` / `\P{name}` (negation). Single-letter short form
`\pX`/`\PX` takes exactly one following character as the name: `\pL` ok, `\pLu` =
`\pL` followed by literal `u` (P-single-Lu); `\px` → `Unknown character property name
{x}`. `\p{}` → `Empty character family`; `\p{L` → `Unclosed character family`.
Usable inside and outside classes; `[^\P{L}]` = `\p{L}` (P-negneg).

**PROP-2 (key=value form).** If the name contains `=`, the part before the first `=` is
the key, the rest is the value (no trimming: `gc =Lu`, `gc= Lu` fail). Keys are
case-insensitive: `gc`, `general_category`, `sc`, `script`, `blk`, `block` (any case:
`GC=Lu`, `Gc=Lu`, `gC=Lu`, `GENERAL_CATEGORY=Lu`, `SC=Latin`, `BLOCK=Greek` all work;
P2-00…09). `generalcategory`, `GeneralCategory`, `is` are not keys. Errors:
`Unknown Unicode property {name=<key>, value=<value>}` where key is shown **lowercased**
(P2-62 shows `generalcategory`).
* `gc=`/`general_category=` value: a general-category name, **case-sensitive**, from the
  category list of PROP-4 (`gc=LC` ok, `gc=L&` no, `gc=Letter` no, `gc=lu` no, `gc=IsL` no).
* `sc=`/`script=` value: accepted iff `Character.UnicodeScript.forName(value)` accepts it
  (SCR-1).
* `blk=`/`block=` value: accepted iff `Character.UnicodeBlock.forName(value)` accepts it
  (BLK-1). `block=InGreek` is rejected (no `In` prefix in this form).

**PROP-3 (name without `=`).** Resolution, as observed (the order matters only for
acceptance, since no name is accepted by two branches with different meanings):
1. If the name starts with `In` (case-sensitive): the rest must be a block name accepted
   by `UnicodeBlock.forName` (BLK-1); otherwise error.
2. If the name starts with `Is` (case-sensitive; `is`, `IS` are not prefixes —
   `\p{isLu}`, `\p{ISLATIN}` fail): let R = rest. Accept if R (compared
   **case-insensitively**) is one of the binary/POSIX-Unicode names of PROP-5; else if R
   is a script accepted by `UnicodeScript.forName` (case-insensitive, SCR-1); else if R is
   one of the case-sensitive names of step 3 (categories, `LC`, `LD`, `all`, `ASCII`,
   `java…` names). Otherwise error.
3. Otherwise the name must be one of (case-sensitive): a general category or group
   (PROP-4), `LC`, `LD`, `all`, a POSIX name (PRE-4/PRE-5: `Lower Upper ASCII Alpha Digit
   Alnum Punct Graph Print Blank Cntrl XDigit Space`), or a `java…` name (PROP-6).
   Script names without `Is` (`\p{Latin}`), binary property names without `Is`
   (`\p{Alphabetic}`, `\p{Assigned}`), `\p{Word}`, `\p{Any}`, `\p{All}`, `\p{L&}` are
   all errors.
Error for unknown names: `Unknown character property name {name}` (index just past
`}`). Whitespace inside braces is significant (`\p{ L}` fails) except in COMMENTS
mode, where whitespace immediately after `{` is skipped (`(?x)\p{ L}` works, but
`(?x)\p{Is Latin}` fails; F-32, X-sp-in-prop) — **partially characterized**.

**PROP-4 (general categories).** Accepted names (case-sensitive, also after `Is` and as
`gc=` values): `Cn Lu Ll Lt Lm Lo Mn Me Mc Nd Nl No Zs Zl Zp Cc Cf Co Cs Pd Ps Pe Pc Po
Sm Sc Sk So Pi Pf` and the groups `L M N Z C P S`, plus `LC` (= Lu ∪ Ll ∪ Lt) and `LD`
(= L ∪ Nd). Long category names (`Uppercase_Letter`) are not accepted anywhere.
`\p{C}` = Cc ∪ Cf ∪ Co ∪ Cs ∪ Cn. Sets equal the UCD 15.0 general categories exactly
(verified for Lu, L, LC, LD, C, Cn, Cs).

**PROP-5 (`Is` binary / Unicode-POSIX names; case-insensitive after `Is`).** Accepted
spellings and their sets (`NameCheck`, 3 016 candidate names; `SetDump` vs UCD):
* `Alphabetic`, `Alpha` → Alphabetic (DerivedCoreProperties).
* `Ideographic` → Ideographic (PropList).
* `Letter` → gc L.
* `Lowercase`, `Lower` → Lowercase (DerivedCoreProperties); `Uppercase`, `Upper` →
  Uppercase; `Titlecase` → gc Lt.
* `Punctuation`, `Punct` → gc P.
* `Control`, `Cntrl` → gc Cc.
* `White_Space`, `WhiteSpace`, `Space` → White_Space (PropList).
* `Digit` → gc Nd.
* `Hex_Digit`, `HexDigit`, `XDigit` → **gc Nd ∪ UCD Hex_Digit** (704 cps; note: this is
  *not* the UCD Hex_Digit property, which has 44 cps — Java's set contains every Nd
  digit, e.g. U+1040; `SetDump`).
* `Join_Control`, `JoinControl` → Join_Control.
* `Noncharacter_Code_Point`, `NoncharacterCodePoint` → Noncharacter_Code_Point.
* `Assigned` → ¬Cn (includes surrogates and private use).
* `Emoji`, `Emoji_Presentation`, `Emoji_Modifier`, `Emoji_Modifier_Base`,
  `Emoji_Component`, `Extended_Pictographic` → emoji-data.txt 15.0. Underscore-free forms
  of these six are **rejected** (`IsEmojiPresentation`, `IsExtendedPictographic`).
* `Alnum` → Alphabetic ∪ Nd; `Graph`, `Print`, `Blank`, `Word` (= `U`-mode `\w`) → the
  PRE-5 / PRE-2 Unicode definitions. `IsWord` exists although `\p{Word}` does not.
Case-insensitive examples: `\p{Isalphabetic}`, `\p{IsWHITE_SPACE}`, `\p{IsLOWER}`,
`\p{Isword}` all work. Not accepted: `IsAny`, `IsAll`, `IsHex`, `IsMath`, `IsDash`,
`IsCased`, `IsCase_Ignorable`, `IsID_Start`, `IsXID_Start`,
`IsDefault_Ignorable_Code_Point`, `IsTitle_Case`, `Is_Alphabetic`, `IsWhite Space`.
The `Is`+POSIX forms ignore the `U` flag (they are always the Unicode definitions):
`\p{IsLower}` = Lowercase even without `U` (P-134).

**PROP-6 (`java…` names; case-sensitive, optional `Is` prefix).** `\p{javaX}` is
`Character.isX(cp)` for X in: `LowerCase UpperCase TitleCase Digit Defined Letter
LetterOrDigit JavaIdentifierStart JavaIdentifierPart UnicodeIdentifierStart
UnicodeIdentifierPart IdentifierIgnorable SpaceChar Whitespace ISOControl Mirrored
Alphabetic Ideographic` (all verified exactly over all code points). Not available:
`javaEmoji…` (although `Character.isEmoji` exists in JDK 21), `javaBmpCodePoint`,
`javaSurrogate`, `javaHighSurrogate`, `javaSupplementaryCodePoint`. `IsjavaDigit` works,
`Isjavadigit`, `JavaDigit` do not. Definitions in UCD terms (Javadoc of
`java.lang.Character`, quoted):
* isLowerCase: "general category type … is LOWERCASE_LETTER, or it has contributory
  property Other_Lowercase" (= Lowercase); isUpperCase analogous (= Uppercase);
  isTitleCase: gc Lt.
* isDigit: "general category type … is DECIMAL_DIGIT_NUMBER".
* isLetter: gc Lu, Ll, Lt, Lm, Lo. isLetterOrDigit: isLetter ∪ isDigit.
* isAlphabetic: gc Lu Ll Lt Lm Lo Nl "or it has contributory property Other_Alphabetic".
* isIdeographic: Ideographic. isMirrored: Bidi_Mirrored.
* isWhitespace: Zs/Zl/Zp except U+00A0, U+2007, U+202F, plus U+0009–000D, U+001C–001F.
* isSpaceChar: gc Zs, Zl, Zp. isISOControl: U+0000–001F, U+007F–009F.
* isIdentifierIgnorable: U+0000–0008, U+000E–001B, U+007F–009F, and gc Cf.
* isJavaIdentifierStart: isLetter, gc Nl, gc Sc, gc Pc.
* isJavaIdentifierPart: letter, Sc, Pc, Nd, Nl, Mc, Mn, or isIdentifierIgnorable.
* isUnicodeIdentifierStart: ID_Start ∪ {U+2E2F} (verified against
  DerivedCoreProperties). isUnicodeIdentifierPart: "Continue := Start + ID_Continue +
  ignorable" per the Javadoc profile of UAX31.
* isDefined: assigned in UnicodeData (= ¬Cn).

**PROP-7 (`all`).** `\p{all}` (lowercase only, also `\p{Isall}`) matches every code point
(1 114 112) including lone surrogates. `\p{All}`, `\p{IsAll}`, `\p{ANY}` → error.

**SCR-1 (scripts).** `\p{IsX}`, `\p{sc=X}`, `\p{script=X}` accept X iff
`UnicodeScript.forName(X)` accepts it. Observed rule (`ForNameCheck`, `ScriptList`;
416 candidates): X is a Unicode 15.0 script long name (with underscores, e.g.
`Old_Italic`) or its 4-letter ISO 15924 alias from PropertyValueAliases.txt,
compared case-insensitively; space-separated or underscore-free forms are rejected
(`IsOldItalic`, `Old Italic`); the aliases `Qaac`, `Qaai`, `Hrkt`,
`Katakana_Or_Hiragana` are rejected; `Zyyy`/`Common`, `Zinh`/`Inherited`,
`Zzzz`/`Unknown` are accepted. Javadoc (UnicodeScript.forName): "Script names and their
aliases are determined by The Unicode Standard. The files Scripts.txt and
PropertyValueAliases.txt define script names and the script name aliases … Character
case is ignored." Membership = Scripts.txt `Script` property (not Script_Extensions):
`\p{IsLatin}` contains U+212A KELVIN SIGN; `\p{IsCommon}` contains `1 _ - U+2028 😀`;
`\p{IsUnknown}` contains Cn, Co and Cs code points (964 861 cps; U-hi-13).

**BLK-1 (blocks).** `\p{InX}`, `\p{blk=X}`, `\p{block=X}` accept X iff
`UnicodeBlock.forName(X)` accepts it (exact agreement over 3 948 candidate spellings).
Javadoc (UnicodeBlock.forName): accepted forms are "Canonical block names as defined by
the Unicode Standard", "Canonical block names with all spaces removed", and "The text
representation of each constant UnicodeBlock identifier … This form replaces all
spaces and hyphens in the canonical name with underscores"; "character case is ignored
for all of the valid block name forms"; "If the Unicode Standard changes block names,
both the previous and current names will be accepted." Observed: `InBasicLatin`,
`InBasic Latin`, `InBASIC_LATIN`, `Inbasiclatin`, `InLatin-1Supplement`,
`InLatin_1_Supplement`, `InLatin Extended-A` accept; `InBasic-Latin`,
`InLatin_Extended-A`, `InLatinExtendedA`, `InGreek_and_Coptic`, `InLatin1Supplement`,
`In Braille Patterns`, double spaces reject. The canonical-form spelling keeps hyphens;
the underscore form replaces both spaces and hyphens. Historic aliases accepted in JDK 21
(found by enumerating public `UnicodeBlock` fields, `BlockAliases`):
`GREEK` (canonical "Greek and Coptic", so `InGreek` works and only the canonical and
spaces-removed forms of the new name, e.g. `InGreekandCoptic`, `InGreek and Coptic`,
work — `InGreek_and_Coptic` does not), `COMBINING_MARKS_FOR_SYMBOLS` (canonical
"Combining Diacritical Marks for Symbols"; also `Combining Marks for Symbols`,
`CombiningMarksforSymbols`), `CYRILLIC_SUPPLEMENTARY` (canonical "Cyrillic Supplement";
also `Cyrillic Supplementary`, `CyrillicSupplementary`), and `SURROGATES_AREA`
(no code points; `\p{InSurrogates_Area}` compiles and matches nothing). Block membership
follows Blocks.txt; code points outside any block belong to no block.

**PROP-8 (lone surrogates).** Properties classify a lone surrogate as gc Cs, script
Unknown, block High/Low Surrogates, Assigned, javaDefined; `\p{C}` matches it; `\p{Cn}`
does not; a surrogate *pair* is classified as its supplementary code point (U-*).

**PROP-9 (case-insensitivity and properties).** See CI-6.

---

## 5. Anchors and boundaries

**ANC-1 (line terminators).** Javadoc: "A newline (line feed) character ('\n'), A
carriage-return character followed immediately by a newline character ("\r\n"), A
standalone carriage-return character ('\r'), A next-line character ('\u0085'), A
line-separator character ('\u2028'), or A paragraph-separator character ('\u2029')." With
`d`, only `\n`. \x0B and \f are never line terminators (D-*-v-*, D-*-f-*).

**ANC-2 (`^`).** Without `m`: matches only at index 0 (including for empty input:
`^` ⟶ "" MATCH). With `m`: matches at index 0 **only if the input is non-empty**, and
after any line terminator unless that terminator ends the input; never between `\r` and
`\n`. Consequences: `(?m)^` ⟶ "" NOMATCH, `(?m)^$` ⟶ "" NOMATCH (D2-caret-empty-m,
D2-caret-dollar-empty-m) — a deviation from the Javadoc wording "^ matches at the
beginning of input"; `(?m)a\n^` ⟶ `a\n` NOMATCH; find of `(?m)^` in `\n\n` gives
positions 0 and 1 only; in `a\r\nb` gives 0 and 3 (D2-caret-m-ends, D-caret-rn-mid-m2).

**ANC-3 (`$` without `m`, and `\Z`).** Matches at end of input, and at the start of a
final line terminator, i.e. at position p when the text from p to the end is exactly one
line terminator (`\r\n` counts as one; the position between `\r` and `\n` of a final
`\r\n` does not match). Find positions: `a\nb\n` → 3, 4; `a\r\n` → 1, 3;
`a\n\n` → 2, 3; with `d`, `a\rb\r` → 4 only (D-dollar-find-*, D-dollar-rn-mid,
D-dollar-double). `\Z` behaves identically to non-multiline `$` regardless of `m`, and
respects `d` (D-Z-find-*, D-d-Z-r-find). Since `matches()` must consume the input,
`a$` ⟶ `a\n` NOMATCH but `a$\n` ⟶ `a\n` MATCH (D-matches-*).

**ANC-4 (`$` with `m`).** Matches at end of input and immediately before every line
terminator, but not between the `\r` and `\n` of a `\r\n` (`a\r\nb\r\n` → 1, 4, 6;
with `md` → 2, 5, 6). `(?m)$` ⟶ "" MATCH.

**ANC-5 (`\A`, `\z`).** `\A` only at index 0 (`a\Ab` never matches; `\A` ⟶ "" MATCH);
`\z` only at end. Unaffected by `m`.

**ANC-6 (`\G`).** For `matches()` (fresh matcher) `\G` succeeds only at index 0
(`\Ga` ⟶ `a` MATCH; `a\Gb` ⟶ `ab` NOMATCH). (Javadoc: "The end of the previous
match".)

**ANC-7 (anchors under quantifiers).** Anchors and lookarounds may be quantified
(`^*`, `\b+`, `(?=a){2}`, `\G*`, `\Z{2}` compile; QNT-6).

**WB-1 (`\b`/`\B` definition).** `\b` holds at position p iff word(left) ≠ word(right),
where at the input start/end the missing side is non-word. `\B` is the negation (so
`\B` ⟶ "" MATCH, `\b` ⟶ "" NOMATCH). `\b` and `\B` are not allowed in classes.
`\B{…}` → `Illegal repetition`; `\b{` followed by anything other than `g}` is parsed as
`\b` + `{` → `Illegal repetition` (D-bx, D-bwb).

**WB-2 (word characters with `U`).** right side: the code point at p (SUR-1);
left side: the code point *ending* at p (a pair is read as one code point). Word =
`U`-mode `\w` (PRE-2). Verified for every code point (`WordCheck start-U`, `end-U`,
`after-space-U`). Lone surrogates are non-word.

**WB-3 (word characters without `U`).** Deviates from the Javadoc's
`(?:(?<=\w)(?=\W)|(?<=\W)(?=\w))`: a non-spacing mark can be a word character.
Define, for a unit index q and the code point c read there:
`isWordAt(q, c)` = c ∈ [a-zA-Z0-9_], **or** (gc(c) = Mn and, scanning units backwards
from q−1 while the unit has gc Mn, the first unit that is not Mn exists and is
`isLetterOrDigit` (gc L* or Nd) when taken as a code point by itself).
* right side of p: c = code point read at p (may be a supplementary Mn).
* left side of p: c = the single **unit** at p−1 (a low surrogate is never a word char).
Verified for every code point in five contexts (`WordCheck`: start, after `a`, after
`é`, after `_`, after space, end-after-`a`). Examples: `á` boundaries {0,2} (no
boundary inside); `é́` {1,2} (é is non-word, the mark is word because é is a
letter); `_́` {0,1}; ` ́` none; `́` alone none; `²́` none
(² is No); `٠́` {1,2}; `a⃝` (Me) {0,1}; `aः` (Mc) {0,1};
`á́b` {0,4}; `a` + U+1D167 (supplementary Mn) → only {0} (right side at 1 is
word because of `a`; left side at 3 reads the low surrogate → non-word);
`a\U0001D167b` → {0,3,4}; `\U0001D400́` → none (the unit before the mark is a low
surrogate). Case-insensitivity does not change `\b` (D3-i-*, D-b-ci-kelvin).

**WB-4 (`\b{g}`).** Grapheme-cluster boundary per GC-1, evaluated with full context on
both sides (the entire input, not just the current match): `e\b{g}́` ⟶ `é`
NOMATCH; `🇦🇧\b{g}🇨` MATCH; `🇦\b{g}🇧🇨` NOMATCH; `👨ZWJ\b{g}👩` NOMATCH; start and end
of input are boundaries (`\b{g}` ⟶ "" MATCH). Agrees with every line of Unicode 15.0
GraphemeBreakTest.txt (602/602, `GraphemeCheck`).

---

## 6. `\R` and `\X`

**LBRK-1 (`\R`).** Matches `\r\n`, or one of `\n \x0B \f \r \u0085 \u2028 \u2029`
(Javadoc: "equivalent to `\u000D\u000A|[\u000A\u000B\u000C\u000D\u0085\u2028\u2029]`").
Unaffected by `d`, `i`. **Backtracking:** when `\R` is not under any quantifier it may
give back the `\n` of `\r\n`: `\R\n` ⟶ `\r\n` MATCH, `(\R)\n` MATCH (group 1 = `\r`),
`\R\R` ⟶ `\r\n` MATCH. When `\R` is the operand of any quantifier (including `{1}`,
`?`, `*`, `+`, and a quantified group whose body is `\R`) it is atomic: `\R{2}`,
`(?:\R){2}`, `\R?\n`, `\R{1}\n`, `\R*\n`, `\R+\n`, `(?:\R)+\n`, `\R++\n` ⟶ `\r\n` all
NOMATCH; `\R{1,2}` and `\R+` ⟶ `\r\n` MATCH (D-R-*, D2-R-*). **Partially
characterized**: an unquantified `\R` inside an atomic group is atomic (`(?>\R)\n`
NOMATCH); `\R` inside an unquantified non-capturing group followed by `\n` was not
probed.

**GC-1 (`\X`).** Matches one extended grapheme cluster as defined by UAX #29 for
Unicode 15.0 (rules GB3–GB13, including GB9a, GB9b, GB11 Extended_Pictographic,
GB12/13 regional-indicator pairing), using Grapheme_Cluster_Break from
GraphemeBreakProperty.txt and Extended_Pictographic from emoji-data.txt, **with an
unpaired surrogate treated as Grapheme_Cluster_Break = Control**. JDK 21 does not
implement the Unicode 15.1 GB9c (Indic conjunct) rule. Verified: all 602 lines of the
15.0 GraphemeBreakTest.txt, plus 10 000 random 1–7-code-point strings drawn from every
GCB value, Extended_Pictographic samples and lone surrogates, against an independent
UAX #29 implementation (`uax29.py`, `graph_cmp.py`): 0 mismatches.

**GC-2 (`\X` starts fresh).** The cluster is computed as if the text began at the
current position: rules needing left context (GB11, GB12/13) do not look before the
start. `e\X` ⟶ `é` MATCH (`\X` matches the lone mark); `.\X` ⟶ three RIs MATCH
(`\X` pairs RI2+RI3); `👨\X` ⟶ `👨ZWJ👩` NOMATCH (`\X` from ZWJ matches only ZWJ,
because GB11 would need the 👨 before the start).

**GC-3 (`\X` atomic).** `\X` never backtracks to a shorter cluster: `\X́` ⟶
`é` NOMATCH, `\X*́` NOMATCH, `\X\n` ⟶ `\r\n` NOMATCH. `\X` on "" fails.
`\X` is not allowed in classes. `\X` in a lookbehind: see LB-5.

---

## 7. Quantifiers

**QNT-1 (counted syntax).** `{n}`, `{n,}`, `{n,m}` with n, m unsigned ASCII decimal
(leading zeros allowed, `a{02}`), 0 ≤ n ≤ m ≤ 2 147 483 647. Errors (Q-syn-*):
`{` not followed by a digit (`a{`, `a{}`, `a{,3}`, `a{x}`, `a{-1}`, `a{ 2}`,
non-ASCII digits `a{٢}`) → `Illegal repetition` @ index of `{`; digits but no proper
close (`a{2`, `a{2,`, `a{2,x}`, `a{2 }`, `a{2, 3}`) → `Unclosed counted closure`;
n > m → `Illegal repetition range`; any number > 2 147 483 647 → `Illegal repetition
range`. In COMMENTS mode whitespace is skipped after the first digit (`(?x)a{1, 2}`,
`(?x)a{1 ,2}` work) but not right after `{` (`(?x)a{ 2}` fails; F-26/27,
X-sp-in-range).

**QNT-2 (greedy, reluctant, possessive).** Suffix `?` = reluctant, `+` = possessive,
on any of `? * + {…}`. Possessive = atomic group around the quantified atom
(`a*+a` ⟶ `aaa` NOMATCH; `(?:ab|a)++b` ⟶ `ab` NOMATCH; Q-poss*).

**QNT-3 (dangling).** `* + ?` with nothing to apply to (pattern start, after `(`, after
`|`, after `(?i)`) → `Dangling meta character 'c'`. After a complete quantifier
(including its `?`/`+` suffix) another `*`, `+` or `?` is also dangling: `a**`, `a*??`,
`a*?+`, `a+++`, `a{2}*`, `x*+*` (Q-syn-22…30).

**QNT-4 (stacked counted quantifiers).** A `{n,m}` immediately after a complete
quantifier is accepted and applies to an **empty** atom; the earlier quantifier is
unaffected: `a{2}{3}` ≡ `a{2}` (`aa` MATCH, `aaaaaa` NOMATCH); `a?{2}` ≡ `a?`;
`a{2,3}?{2}` ≡ `a{2,3}?`; `a{1}{2}` ≡ `a` (Q4-*).

**QNT-5 (counted quantifier with no atom).** `{n,m}` at pattern start, after `(`/`|`, or
after an inline-flag group applies to the empty string: `{2}` ⟶ "" MATCH and ⟶ `{2}`
NOMATCH; `(?i){2}a` ⟶ `a` MATCH (Q4-brace-start-*, Q4-group-start-quant). (`{` *not*
forming a valid closure at pattern start is `Illegal repetition` @1.)

**QNT-6 (quantifying zero-width items).** Quantifiers on anchors, `\b`, `\b{g}`, `\G`,
lookarounds are accepted and behave as repeated assertions (`\b{2}a`, `(?=a){2}a`,
`^*a`, `a$*` ⟶ MATCH on `a`; Q4-*).

**QNT-7 (large counts).** `a{2147483647}` compiles and fails on `a`; `a{0,2147483647}` ⟶
`aaa` MATCH. `(?:a|b){2147483647}` ⟶ `ab` NOMATCH without error.

**QNT-8 (zero-length iterations).** A loop iteration that consumes nothing ends the loop
successfully; the iteration's captures are kept. `(a?)*` ⟶ `aa` g1=[2,2];
`(a*)*b` ⟶ `b` g1=[0,0]; `(a|)*b`, `(|a)*b` ⟶ `aab` g1=[2,2]; `(a?){3}` ⟶ `a`
g1=[1,1]; `(?:()|(a))*` ⟶ `a` g1=[1,1] g2=[0,1] (Q-empty-star*). Exception: CAP-5.

---

## 8. Groups and inline flags

**GRP-1 (kinds).** `(X)` capturing; `(?<name>X)` named capturing; `(?:X)` non-capturing;
`(?>X)` atomic; `(?=X) (?!X) (?<=X) (?<!X)` lookarounds;
`(?flags)` and `(?flags:X)`. Groups are numbered by opening parenthesis, named groups
included. Not supported (all `Unknown inline modifier` @ index of the character after
`?`): `(?P<n>…)`, `(?'n'…)`, `(?P=n)`, `(?&n)`, `(?#comment)`, `(?(1)…)`, `(?^i)`,
`(?|…)` (Z-branchreset). `()` and `(?:)`, `(?>)`, `(?=)`, `(?!)`, `(?<=)`
are valid empty groups.

**GRP-2 (group names).** `[A-Za-z][A-Za-z0-9]*`, case-sensitive, any length. Errors:
first char not an ASCII letter (digit, `_`, `é`, empty) → `capturing group name does not
start with a Latin letter`; a non-alphanumeric character (`_`, space) or end before `>`
→ `named capturing group is missing trailing '>'`; duplicate →
`Named capturing group <a> is already defined`.

**GRP-3 (unbalanced).** Missing `)` → `Unclosed group` (index = pattern length);
extra `)` → `Unmatched closing ')'` (index = position of `)` − 1, so `)` alone has
index −1).

**FLG-1 (letters).** `i` CASE_INSENSITIVE, `d` UNIX_LINES, `m` MULTILINE, `s` DOTALL,
`u` UNICODE_CASE, `x` COMMENTS, `U` UNICODE_CHARACTER_CLASS, and `c` (FLG-6). Syntax:
letters, optionally `-` and letters, then `)` or `:`. Empty sets are allowed: `(?)`,
`(?-)`, `(?i-)`, `(?-:a)`. Repeated letters OK (`(?ii)`, `(?i-i)`). A second `-` or a
`+` → `Unknown inline modifier`. Any other character (`l n a J X I S #` …) →
`Unknown inline modifier`. Missing `)` after letters at end → `Unknown inline
modifier`.

**FLG-2 (scope of `(?flags)`).** A bare flag group changes the flags from that point to
the end of the **innermost enclosing group** (capturing or not), and the change carries
across `|` into later alternatives of the same group. At the end of the enclosing group
the flags revert. Examples (F-*): `a(?i)b` ⟶ `aB` MATCH, `AB` NOMATCH;
`(?i)a|b` ⟶ `B` MATCH; `a|(?i)b|c` ⟶ `C` MATCH; `x(?i)|y` ⟶ `Y` MATCH;
`(?i)(?:a|b)(?-i)|c` ⟶ `C` NOMATCH; `((?i)a)|b` ⟶ `B` NOMATCH;
`((?i)a|b)` ⟶ `B` MATCH; `(a(?i)b)c` ⟶ `aBC` NOMATCH; `(?:a(?i))b` ⟶ `aB` NOMATCH.
Javadoc: "embedded flags always take effect at the point at which they appear, whether
they are at the top level or within a group; in the latter case, flags are restored at
the end of the group just as in Perl."

**FLG-3 (`(?flags:X)`).** Flags apply to X only (`(?i:a)b` ⟶ `AB` NOMATCH).

**FLG-4 (interactions).** `U` implies UNICODE_CASE (Javadoc: "The flag implies
UNICODE_CASE"); `(?U)é` with `i` ⟶ `É` MATCH. Turning off: `(?-U)` turns off both `U`
and UNICODE_CASE; `(?-u)` turns off UNICODE_CASE but leaves `U` classes on
(`(?-u)\w` with `U` ⟶ `é` MATCH; `(?-u)é` with `iU` ⟶ `É` NOMATCH) (F-79…83). `u`
without `i` has no visible effect.

**FLG-5 (when a flag is read).** Case-insensitivity of a literal, class, property or
back-reference is fixed by the flags in effect at that atom (`(a)(?i)\1` ⟶ `aA` MATCH;
`(?i)(a)(?-i)\1` ⟶ `aA` NOMATCH; R-48…52). `$`, `^`, `.` likewise use the flags at
their position (`(?s:.)a` ⟶ `\na` MATCH; `(?s:.).` ⟶ `\n\n` NOMATCH).

**FLG-6 (`c`).** The letter `c` is accepted in flag groups (`(?c)`, `(?-c)`, `(?c:…)`)
but has **no observable effect**: `(?c)å` ⟶ `å` NOMATCH (whereas the
CANON_EQ compile flag gives MATCH) (CE-1…5). The Javadoc for CANON_EQ says "There is no
embedded flag character for enabling canonical equivalence." There is no letter for
LITERAL (`(?l)` is an error).

**XM-1 (COMMENTS mode).** With `x`: the whitespace characters
U+0020, U+0009, U+000A, U+000B, U+000C, U+000D (only these; not U+0085, U+00A0, U+2028,
U+3000, U+001C–001F, etc.) are ignored, and `#` starts a comment that runs up to and
including the next `\n` or `\r` (not U+0085/U+2028) or to the end (X-ws-*, X-cm-*). An
escaped space `\ ` and `\#` are literals. Inside classes see CLS-10.

**XM-2 (where whitespace is skipped).** Whitespace is skipped between tokens and also
inside many multi-character constructs (observed): between an atom and its quantifier
(`a {2}`), between quantifier and `?`/`+` suffix, after `(` and after `(?` (`( ?:a)`,
`(? :a)`), inside group names (`(?<n >a)`), between the digits of `\x` / `\u` / octal /
back-reference numbers (`\x4 1` = `A`, `\u00 41` = `A`, `\0 101` = `A`, `\1 0` = `\10`
when ≥10 groups), inside `\x{ 41 }`, between `\p`/`\k`/`\b` and `{`/`<`
(`\p {L}`, `\k <n>`, `\b {g}`), between `&` and `&`. Not skipped: after `\` (the next
character is taken raw: `\ a` = space then `a`; `\c a` = `\c` of space), inside
`\N{…}` beyond trimming, inside property names after the first character (`\p{Is Latin}`
fails), right after `{` of a counted quantifier, and before a flag letter while `x` is
not yet active (`(?i x)` fails, `(?x i)` works). `(? <=a)` fails while `(?< =a)` works.
These are recorded observations (F-*, X-sp-*); treat the boundary as **partially
characterized**.

---

## 9. Lookaround

**LA-1 (lookahead).** `(?=X)`/`(?!X)` are zero-width, atomic (no backtracking into X
once the lookahead succeeded). Captures: CAP-3.

**LB-1 (lookbehind compile-time width).** The body of `(?<=X)`/`(?<!X)` must have a
computable maximum width, else `PSE "Look-behind group does not have an obvious maximum
length"`. Observed acceptance (G-look-*, L-syn-*, W3-*):
* Accepted: literals, classes, `.`, properties, alternation, bounded quantifiers on
  anything, bounded/unbounded *possessive and lazy* quantifiers on single-character
  atoms, **unbounded quantifiers (`*`, `+`, `{n,}`) applied directly to a
  single-character atom** (`a*`, `\p{L}+`, `[a-z]{2,}`, `.*`, `a*b*c*`, `\Qab\E*`),
  also when the atom is wrapped in one group (`(?:a)*`, `(a)*`), `\X*`, `\X+`,
  quantified zero-width items (`(?:)*`, `()*`, `\b*`, `(?=ab)*`), nested lookarounds
  (even containing unbounded groups: `(?<=(?=(?:ab)*))`), `\R`, `\b{g}`, `^`, `$`,
  `\G`, unquantified atomic groups (`(?<=(?>a+))` is accepted).
* Rejected: any back-reference (`(a)(?<=\1)`, `\k<n>`); unbounded quantifiers on
  multi-atom or alternation groups (`(?:ab)*`, `(?:ab)+`, `(?:ab){2,}`, `(?:a|b)*`,
  `(a|b)*`, `(?>ab)*`, `(?:a*)*`, `(?:\X)*`, `(?:ab)*+`); `\R+`; a nested lookbehind
  with such a body.
* Overflow: the maximum is accumulated in 32-bit signed arithmetic. Observed: bounded
  repeats whose product/sum exceeds 2 147 483 647 are rejected
  (`(?:ab){1073741824}`, `a{1073741824}b{1073741824}`, `x{2000000000}y{147483648}`,
  `(?:x{1000000000}){2}z{147483648}`), but sums involving plain literal sequences or
  alternations are not checked (`a{2147483647}bc`, `x{2000000000}yyy`,
  `(?:x{2000000000}|y)z{147483648}` compile); an unbounded single-atom quantifier
  followed by a counted quantifier is rejected (`a*b{2}`, `a{0,2147483647}b{2}`,
  `a{2147483646}b{2}`) while followed by a literal or another single-atom `*` it is
  accepted (`a*b`, `a*b*`). **Partially characterized** — recommended implementation:
  accept exactly the shapes listed above, treat `{n,2147483647}` as unbounded, and
  reject any bounded total ≥ 2^31.

**LB-2 (lookbehind width model).** For matching, each lookbehind body has a width
interval [minW, maxW] measured in UTF-16 units, computed structurally:
* a single-code-point matcher (literal character, class, `.`, property, predefined
  class) has width [1, K], where **K = 2 if the pattern string contains any UTF-16
  surrogate unit anywhere** (a literal supplementary character, a literal lone
  surrogate, even inside a comment or `\Q…\E`), and **K = 1 otherwise** (escapes such as
  `\x{1F600}`, `😀`, `\N{GRINNING FACE}` do not count);
* sequence: sum; alternation: [min of mins, max of maxes]; `{n,m}` on X:
  [n·minX, m·maxX]; unbounded: maxW = ∞ (scanning is bounded by the input start);
* zero-width items (anchors, `\b`, `\b{g}`, lookarounds, empty groups): [0, 0];
* `\R`: [1, 2]; `\X`: **[0, 0]** (LB-5).
Evidence: W-*, W2-*, L2-*. E.g. with K=1: `\x{1F600}(?<=^\x{1F600})` ⟶ `😀` NOMATCH,
`.(?<=^.)` ⟶ `😀` NOMATCH, `(?:.{1,2})(?<=^(?:.{1,2}))` ⟶ `😀` MATCH; with K=2 (literal
😀 in the pattern): `😀(?<=^😀)` MATCH, `(?:.(?<=^.))😀?` ⟶ `😀` MATCH,
`.(?<=^.)\uD83D?` written with a *literal* lone D83D ⟶ `😀` MATCH, and even
`(?x).(?<=^.) #😀` (😀 only in a comment) ⟶ `😀` MATCH (W2-dot-lit-comment).

**LB-3 (lookbehind matching).** At position i, try start positions
s = i − minW, i − minW − 1, …, down to max(i − maxW, 0) **in that order (nearest
first)**; for each s, run the body forwards from s and accept only a body match that ends
exactly at i. The first success wins (its captures are kept, CAP-3). Captures show the
order: `(?<=(a|aa))b` ⟶ `aaab` g1="a"; `(?<=(a+))b` g1="a"; `(?<=(a*))b` g1="" ;
`(?<=^(a*))b` g1="aaa" (L-3…10). Negative lookbehind succeeds iff no s works.

**LB-4 (start positions are UTF-16 units).** Because s steps by units, s can fall on the
low half of a surrogate pair; the body then reads a lone low surrogate there (SUR-1).
Hence (K = 1): `(?<=.)x` ⟶ `😀x` succeeds with the body matching the lone `\uDE00`
(capture `(?<=(.))x` g1="\uDE00", L-sur-41); `(?<=\P{L})x`, `(?<=[^a])x`, `(?<=\S)x`
succeed on `😀x`; `(?<=\p{So})x`, `(?<=[\x{1F600}-\x{1F64F}])x`, `(?<=\x{1F600})x`,
`(?<=^.)x` fail on `😀x`. A lone-surrogate escape in a lookbehind can match half of a
pair: `(?<=\uDE00)x` ⟶ `😀x` succeeds; `\x{1F600}(?<=\uDE00)` ⟶ `😀` MATCH
(L-sur-05, L-sur-m1); `(?<!\uDE00)x` fails on `😀x`.

**LB-5 (`\X` in lookbehind).** `\X` has width [0,0] in a lookbehind, so a positive
lookbehind containing a required `\X` never succeeds (`(?<=\X)x` ⟶ `ax` fails;
`(?<=\X*)` only succeeds with zero clusters) and `(?<!\X)` always succeeds
(W-X-*, W2-X-neg*).

**LB-6 (case-insensitive lookbehind).** Flags inside the body apply normally
(`(?<=(?iu)^𐐀)x` ⟶ `𐐨x` succeeds; with `(?i)` only it fails, L-sur-47/48).

---

## 10. Back-references

**BR-1 (numbered).** `\n`: the parser takes the first digit, then keeps appending
following digits while the resulting number does not exceed the number of groups
**opened so far** (Javadoc: "a larger number is accepted as a back reference if at least
that many subexpressions exist at that point in the regular expression, otherwise the
parser will drop digits until the number is smaller or equal to the existing number of
groups or it is one digit"). `(a)\10` = `\1` + `0`; with 10 groups `\10` = group 10 and
`\11` = `\1` + `1`; with 11 groups `\111` = `\11` + `1` (R-4…7, R-77/78). A one-digit
reference to a group that does not exist (yet or at all) compiles: `(a)\2`, `\9`, `\1`
alone compile and simply fail to match; forward references work in loops
(`(\2b|(a))+` ⟶ `aab` MATCH, R-27).

**BR-2 (named).** `\k<name>`; the name must already have been defined **earlier in the
pattern text**, otherwise `named capturing group <x> does not exist` (`\k<a>(?<a>x)`
fails). Same name rules as GRP-2. A named group is also reachable by number.

**BR-3 (unset group).** A back-reference to a group that has not captured fails
(`(a)?\1` ⟶ "" NOMATCH; `(a)|\1b` ⟶ `b` NOMATCH; `(a\1)` ⟶ `a` NOMATCH; `(a\1?)` ⟶ `a`
MATCH).

**BR-4 (reference inside its own group / earlier iteration).** Inside a repeated group,
`\n` refers to the value from the previous completed iteration: `(a|b\1)+` ⟶ `aba` MATCH
(g1 ends "ba"); `((a)\2)+` ⟶ `aaaa` MATCH; `(?:x(\1?a))+` ⟶ `xaxaa` MATCH (oracle data).

**BR-5 (comparison, case-sensitive).** Compares the captured UTF-16 units with the
input units at the current position (no pairing check): `(\uD83D)\1\uDE00` ⟶
D83D D83D DE00 MATCH — the back-reference consumed the high half of a pair and the
lone-escape matched the low half (R-55). `(.)\1` ⟶ `😀😀` MATCH.

**BR-6 (comparison, case-insensitive).** With `i` (no `u`): ASCII letters compare
case-insensitively, everything else exactly (`(é)\1` ⟶ `éÉ` NOMATCH). With `iu` (or
`U`): code point by code point, c ≈ d iff L(U(c)) = L(U(d)) (verified for every pair in
the case-closure candidate set, `CaseCheck`/`CaseDump`: 2 984 pairs, 0 exceptions).
Thus `(ß)\1` ⟶ `ßẞ` MATCH both ways, `(k)\1` ⟶ `kK`(U+212A) MATCH, `(i)\1` ⟶ `iİ` MATCH,
`(𐐀)\1` ⟶ `𐐀𐐨` MATCH; no multi-character folding (`(ß)\1` ⟶ `ßSS` NOMATCH,
`(ss)\1` ⟶ `ssß` NOMATCH). The flags used are those at the back-reference (FLG-5).

**BR-7 (quantified / possessive).** Back-references can be quantified like atoms
(`(a)\1{2}`, `(a)\1+`, `(a)\1*?b` MATCH; `(a)\1++a` ⟶ `aaa` NOMATCH).

**BR-8 (not in lookbehind).** Any back-reference inside a lookbehind is a compile error
(LB-1).

---

## 11. Captures and backtracking

**CAP-1 (retention across iterations).** A group keeps its last successful capture
when a later iteration does not set it (Javadoc: "Matching the string "aba" against the
expression (a(b)?)+, for example, leaves group two set to "b""). Inner groups are not
reset at the start of an iteration: `((a)|b)+` ⟶ `ab` g2="a"; `(?:(a)|(b))+` ⟶ `aba`
g1="a"(2,3) g2="b" (Q-nest*).

**CAP-2 (restoration on backtracking).** Captures made by ordinary groups (outside
lookaround/atomic/possessive constructs) are undone when the engine backtracks out of the
group: `(?:(a)x|\1)` ⟶ `a` NOMATCH; `(?:(a)x|a)\1` ⟶ `aa` NOMATCH; `(?:(a)b)*a\1` ⟶
`aba` NOMATCH (K-00…05, K-33…56).

**CAP-3 (commitment in lookaround, atomic and possessive constructs).** When the body of
a positive or negative lookahead, a lookbehind, an atomic group, or a possessively
quantified atom **succeeds**, the captures it made are committed: they are **not**
undone if the engine later backtracks to before that construct (e.g. into another
alternative). If the body **fails**, its captures are rolled back. Examples (K-*):
`(?:(?=(.))x|\1a+)` ⟶ `aa` MATCH; `(?:(?=(a))x|\1)` ⟶ `a` MATCH;
`(?:(?>(a))x|\1)`, `(?:(a)?+x|\1)`, `(?:(a)*+x|\1)`, `(?:(a)++x|\1)`, `(?:(a){1}+x|\1)`,
`(?:a(?<=(a))x|\1)` ⟶ `a` MATCH; `(?:(?!(a))x|\1)` ⟶ `a` MATCH (negative lookahead
failed because its body matched — captures kept); `(?:(?!(a)b)x|\1)` ⟶ `a` NOMATCH
(body failed); `(?:(?>(a)x)|\1)`, `(?:(?=(a)x)|\1)` ⟶ `a` NOMATCH (body failed). An
ordinary group around a committed construct is itself still restored:
`(?:((?=(a)))x|\2)` ⟶ `a` MATCH with g1 unset, g2="a". Committed captures persist
through optional/loop exits: `(?:(?=(a))b)?a\1` ⟶ `aa` MATCH.

**CAP-4 (lookaround captures visible afterwards).** `(?=(a))\1` ⟶ `a` MATCH;
`a(?<=(a))\1` ⟶ `aa` MATCH; `(?!(a))\1` ⟶ `a` NOMATCH (the lookahead itself fails).

**CAP-5 (zero-width fixed body quirk).** Let G be a capturing group whose body can only
match the empty string *and has fixed width* — a sequence of zero-width items without
alternation or variable quantifiers (`()`, `(\b)`, `(^)`, `($)`, `(\G)`, `(\b{g})`,
`((?=a))`, `((?!x))`, `(\b\B)`, `(a{0})`, `(x{0,0})`, `((?:))`). If G is quantified with
a **greedy or reluctant quantifier with minimum 0 and maximum ≥ 2** (`*`, `*?`, `{0,n}`
n≥2, `{0,}?`), then zero-length iterations of G do not record G's capture; inner groups
evaluated in the body do record. With min ≥ 1 (`+`, `{1,3}`, `{2,}`), with `?`/`{0,1}`
(greedy), and with possessive `*+`, the capture is recorded. Bodies with alternation
or variable width (`(|)`, `(\b|x)`, `(x?)`) record normally (QNT-8). Evidence (Q2-*,
Q3-*): `(\b)*a` ⟶ `a` g1 unset; `(\b)+a` g1=[0,0]; `(\b)?a` g1=[0,0]; `(\b){0,1}a`
g1=[0,0]; `(\b){0,2}a` unset; `(\b)*+a` g1=[0,0]; `()*a` unset; `(|)*a` g1=[0,0];
`((\b))*a` g1 unset, g2=[0,0]. Boolean consequence: `(\b)*a\1` ⟶ `a` NOMATCH but
`(x?)*a\1` ⟶ `a` MATCH (Q3-20/21).

---

## 12. Case-insensitive matching

Let `i` = CASE_INSENSITIVE, `iu` = CASE_INSENSITIVE with UNICODE_CASE (or with `U`,
which implies UNICODE_CASE; `iU` results are identical to `iu`, `CaseCheck`).

**CI-1 (ASCII-only `i`).** Only ASCII letters a–z/A–Z fold to each other; every other
character matches only itself (`(?i)é` ⟶ `É` NOMATCH; `(?i)k` ⟶ U+212A NOMATCH;
`(?i)\x{10400}` ⟶ `𐐨` NOMATCH). Javadoc: "By default, case-insensitive matching assumes
that only characters in the US-ASCII charset are being matched." (Verified on all 52
ASCII letter pairs and every non-ASCII candidate pair.)

**CI-2 (`iu` literal characters and one-character classes).** A pattern character p
matches input character t iff t = p, or [p is *cased*, i.e. U(p) ≠ p or L(p) ≠ p] and
[U(t) = U(p) or L(U(t)) = L(U(p))]. The guard is asymmetric: `(?iu)ß` matches only `ß`
(ß has no simple mappings) while `(?iu)ẞ` matches `ß` and `ẞ`. Verified on every pair
within 3 mapping-hops (2 984 pairs; the only pair the unguarded formula gets wrong is
p=U+00DF, t=U+1E9E). Same rule for a single-member class `[p]` and for `\Q…\E` text.
Examples (CI-l*): `k`, `K`, U+212A all match {K, k, U+212A}; `s`, `ſ` match {S, s, ſ};
`i`, `I`, `İ`, `ı` all match {I, i, İ, ı}; `ς` matches {Σ, σ, ς}; `ǅ` matches
{Ǆ, ǅ, ǆ}; Cherokee U+13A0 ≈ U+AB70; Georgian U+10D0 ≈ U+1C90 (not U+10A0);
`\x{10400}` ≈ U+10428. No multi-character folding (`ß` never matches `ss`).

**CI-3 (ranges).** A range [a-b] (including one-character ranges `[c-c]`) matches t iff
a ≤ t ≤ b, or a ≤ U(t) ≤ b, or a ≤ L(U(t)) ≤ b (`iu`, verified exactly on all
candidate pairs). With ASCII `i`, the extra tests apply only when t is an ASCII letter,
using its ASCII other-case form. Ranges therefore differ from CI-2:
`(?iu)[K-K]` matches {K,k} but not U+212A; `(?iu)[k-k]` matches {K,k,U+212A};
`(?iu)[I-I]` matches {I,i,ı} but not İ; `(?iu)[ß-ß]` matches {ß, ẞ};
`(?iu)[ẞ-ẞ]` matches only ẞ; `(?iu)[ς-ς]` matches only ς; `(?iu)[ǅ-ǅ]` only ǅ;
`(?iu)[ſ-ſ]` only ſ; `(?iu)[a-z]` matches A–Z, a–z, ſ, U+212A, İ, ı;
`(?iu)[A-Z]` matches A–Z, a–z, ſ, ı (not U+212A, not İ);
`(?i)[Z-a]` = {A, Z, [, \, ], ^, _, `, a, z}; `(?i)[ⓐ-ⓒ]` (non-ASCII, no `u`) matches
only itself (CI-r*, CI-circ*).

**CI-4 (multi-member classes).** Each member is case-closed by its own rule (CI-2 for
single characters, CI-3 for ranges, CI-6 for properties), the class is the union /
intersection of those closures, and `^` complements the result (CLS-8, CLS-9):
`(?i)[a-z&&[^b]]` matches {a,A,…} without b/B; `(?i)[A-Z&&[a-c]]` = {a,A,b,B,c,C}.

**CI-5 (predefined classes).** `\d \s \w \h \v \R \X` and their negations are not
affected by `i`/`iu` (e.g. `(?iu)\w` does not match U+212A; B2-ci-w).

**CI-6 (properties under `i` or `iu`).** Most properties are unaffected
(`\p{L}`, scripts, blocks, `\p{Alpha}`, `\p{ASCII}`, `\p{InBasicLatin}`,
`\p{IsAlphabetic}`, …: `PropCheck ci`). The case-related ones change, **with or without
UNICODE_CASE** (`i` alone suffices):
* `\p{Lu}`, `\p{Ll}`, `\p{Lt}` (any spelling: `IsLu`, `gc=Lu`, `general_category=Ll`)
  → gc LC = Lu ∪ Ll ∪ Lt (4 095 cps). `\p{LC}` unchanged.
* `\p{IsLowercase}`, `\p{IsUppercase}`, `\p{IsTitlecase}`, `\p{IsLower}`,
  `\p{IsUpper}`, `\p{javaLowerCase}`, `\p{javaUpperCase}`, `\p{javaTitleCase}` →
  Lowercase ∪ Uppercase ∪ gc Lt (4 526 cps).
* POSIX `\p{Lower}`, `\p{Upper}` without `U` → [A-Za-z] (also with `iu`); with `U` →
  Lowercase ∪ Uppercase ∪ Lt (as above).
`\P{…}` and `[^\p{…}]` are complements of these closed sets: `(?i)\P{Lu}` does not
match `a` or `A`; `(?i)[\p{L}&&[^\p{Lu}]]` matches no cased letter (B2-ci-*). Identical
on JDK 25 (hypothesis table unchanged).

---

## 13. Surrogates and unpaired surrogates

**SUR-1 (reading).** Every single-character matcher (literal, class, `.`, property,
predefined class) at unit index i reads the code point at i as defined in §0.2 and, if
it matches, consumes 1 or 2 units. A lone surrogate matches a matcher that contains it
(`.`, `[^a]`, `\P{L}`, `\p{Cs}`, `[\uD800-\uDFFF]`, `\W`, `\D`, `\S`, `\H`, `\V`,
`\p{IsAssigned}`, `\p{C}`, `\X`) (U-hi-*, U-lo-*). A surrogate pair is never matched
by a matcher for its halves: `\uD83D` ⟶ `😀` NOMATCH, `[\uD83D]` NOMATCH,
`[\uD800-\uDFFF]+` NOMATCH, `.\uDE00` NOMATCH, `[^a]{2}` NOMATCH, `..` NOMATCH
(A-u-lone-*, B-class-lonehi-*, U-pair-*).

**SUR-2 (reversed / broken sequences).** Input low+high (`\uDE00\uD83D`) is two lone
surrogates (`..` MATCH, `.` NOMATCH); high + non-surrogate is a lone high surrogate
followed by that character (`\uD83D.` ⟶ `\uD83Da` MATCH).

**SUR-3 (positions inside pairs).** Matching can only reach a unit index inside a pair
via a back-reference (BR-5) or a lookbehind start position (LB-4); at such an index the
low surrogate is read as a lone code point.

**SUR-4 (literal pairs in patterns).** A literal supplementary character in the pattern
string (written as two UTF-16 units) is one code point; quantifiers apply to it as a
whole (`😀{2}` ⟶ `😀😀` MATCH). Its presence switches lookbehind widths (LB-2, K=2).

---

## 14. CANON_EQ and LITERAL (not reachable from Cypher)

**CEQ-1.** Only via the compile flag. Observations (CE-6…16), recorded without
characterization: `å` ⟷ `å` match both ways; `[å]` ⟶ `å` MATCH;
`[å]` ⟶ `å` MATCH; `ạ̊` ⟶ `ạ̊` MATCH (reordering);
`.` ⟶ `å` NOMATCH; `a` ⟶ `å` NOMATCH; `Å` (U+00C5) ⟶ `Å` (U+212B) NOMATCH;
group captures span the decomposed input. Implementation note (Javadoc): "If CANON_EQ is
specified and the number of combining marks for any character is too large, an
OutOfMemoryError is thrown."

**LIT-11 (LITERAL flag).** Pattern taken verbatim; only CASE_INSENSITIVE and
UNICODE_CASE keep an effect (Javadoc; A-L-*).

---

## 15. Resource behaviour

**RES-1 (match-time stack overflow).** `matches()` throws `StackOverflowError` when a
loop whose body is *not* fixed-width (contains alternation, a variable quantifier, a
lazy/optional element, or a capture of such) iterates many times. With the default
thread stack (JDK 21, x86-64 Linux, `java -cp … Probe`, main thread), the threshold is
between 1 000 and 1 500 iterations for `(?:a|b)*`, between 1 500 and 2 000 for `(a|b)*`,
and between 3 000 and 4 000 for `(?:a{1,2})*`. Shapes that overflowed at 10 000
characters (S-*): `(a|b)*`, `(?:a|b)*`, `(?:a|b)*?`, `(?:a|b)+`, `(?:a|b){0,10000000}`,
`(?>(?:a|b)*)`, `(?:a|b)*c`, `(?:x?a)*`, `(?:a|ab)*`, `(?:a{1,2})*`, `(?:(a)|b)*`.
Shapes that did **not** overflow at 1 000 000 characters: `a*`, `.*`, `.*?`, `a*?`,
`[ab]*`, `[^x]*`, `(a)*`, `(?:a)*`, `(ab)*`, `(?:ab)*`, `(?:a{2})*`, `(?:[ab])*`,
`(?:\w)*`, `(?:.)*`, `(?s:.)*`, `\X*`, `(?:a*)*`, `(?:a+)+`, `(?>a|b)*` (atomic body
counts as fixed width), `(?:a|b)*+` and `(?:a|b)*+c?` (possessive), `(a)\1*`,
`(?:(?=a)a)*`, `a{0,10000000}`. A long *sequence* of optional items also overflows:
the pattern `a?a?…a?` (10 000 copies) on 10 000 `a`s ⟶ StackOverflowError at match
time (S2-qseq-10000); 10 000 plain literals do not (S2-seq-*).

**RES-2 (thresholds are environmental).** The exact threshold depends on thread stack
size and JIT state; it is not part of the contract. Recommendation for the
reimplementation: use an explicit backtracking stack (no recursion) and impose a
documented step/stack budget that, when exceeded, raises a distinct error, rather than
attempting to reproduce Java's thresholds. Patterns that Java can never match without
overflow (e.g. `(?:a|b)*` on 1 500+ characters) will then differ from Java (Java:
error; implementation: true/false). This is a deliberate, documentable divergence.

**RES-3 (compile-time stack overflow).** Deep nesting makes `Pattern.compile` throw
`PatternSyntaxException` "Stack overflow during pattern compilation" (index varies by
run/JDK). Observed: 1 000 nested `(?:` or `(?=` groups → error; 1 000 nested `(` → OK,
3 000 → error; 1 000 nested `[` → OK, 10 000 → error; 10 000 `&&` operands → error;
100 000 `a?` → error; 100 000 top-level alternatives `a|a|…` → OK; 100 000 literal
characters → OK (S2-*). Recommendation: impose a documented nesting limit (e.g. 500)
with its own error, and iterative parsing for flat constructs.

---

## 16. JDK 25 differences (vs JDK 21)

**J25-1.** Unicode 16.0 data: all property sets grow (e.g. `\p{L}` 136 104 → 141 028,
`\p{Cn}` 825 345 → 819 533, `\d`/`\p{Nd}` 680 → 760), grapheme clusters follow Unicode
16.0 including GB9c (JDK 21 fails 7 of the 1 093 Unicode 16.0 test lines, e.g.
`÷ 0915 × 094D × 094D × 0924 ÷`; JDK 25 passes all; JDK 21 passes all 602 Unicode 15.0
lines). No difference in any of the 3 153 syntax/semantics probes other than the
nondeterministic index of "Stack overflow during pattern compilation". The case-closure
and property-name-resolution rules are unchanged (`PropCheck ci` hypothesis table
identical).

---

## 17. Open and unverified points

1. `\N{…}` for C0/C1 controls and other `<control>`-named code points: only `NULL`
   verified (LIT-8). Generate the name table from JDK 21 `Character.getName`.
2. `\cX` for supplementary X (LIT-9).
3. CLS-7 odd/even `&`-run rule is a generalization from 9 probes; runs longer than 5
   and runs adjacent to nested classes were not probed.
4. `\R` backtracking when wrapped in an unquantified non-capturing group (LBRK-1).
5. Lookbehind width overflow rules (LB-1) only partially characterized.
6. COMMENTS-mode whitespace inside constructs (XM-2) partially characterized.
7. CANON_EQ semantics not characterized (out of scope).
8. CAP-5 boundary for bodies such as `(?:)`-wrapped lookarounds was probed only for the
   listed shapes.
9. (resolved) `(?|…)` is `Unknown inline modifier`.
10. Stack-overflow thresholds are environment-specific (RES-2).
11. Error indices (Appendix C) are recorded per probe but not reduced to a rule.

---

## Appendix A. Sources

Documentation (fetched 2026-10-21, JDK 21 API, docs.oracle.com):
* https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/regex/Pattern.html
* https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/regex/Matcher.html
* https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/regex/PatternSyntaxException.html
* https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Character.html
* https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Character.UnicodeBlock.html
* https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Character.UnicodeScript.html

Unicode data (https://www.unicode.org/Public/15.0.0/ucd/): UnicodeData.txt,
PropertyAliases.txt, PropertyValueAliases.txt, Blocks.txt, Scripts.txt, PropList.txt,
DerivedCoreProperties.txt, CaseFolding.txt, SpecialCasing.txt,
emoji/emoji-data.txt, auxiliary/GraphemeBreakProperty.txt,
auxiliary/GraphemeBreakTest.txt; and 16.0.0 auxiliary/GraphemeBreakTest.txt.
Standards: UTS #18 (Unicode Regular Expressions), UAX #29 (Unicode 15.0 revision),
UAX #44, UAX #31.

Runtimes: JDK 21 = Eclipse Temurin 21.0.9+10 JRE; JDK 25 = OpenJDK 25.0.4.1 (Red Hat
build). Probe programs compiled with `javac --release 21`.

Black-box oracle data consulted (outputs only, not code):
`crates/uni-query-functions/testdata/java_regex_oracle.tsv` (it led to probe set K /
CAP-3).

Not consulted: any OpenJDK source, decompiled class, the existing Rust implementation in
`crates/uni-query-functions/src/java_regex/`, or internal design documents about it.

## Appendix B. Probe programs and log

All probe material is in `/home/rohit/tmp-tests/cleanroom_spec/` (outside the repo):
* `Probe.java` — reads `id, mode, flags, pattern(UTF-16 hex), input(UTF-16 hex)` lines;
  mode `M` = `matches()`, `F` = all `find()` results, `C` = compile only,
  `S` = per-code-point `matches()` over the input's code points; prints groups, PSE
  description/index, or caught `StackOverflowError`. Uses only the public API.
* `run.py` — runs a probe file on JDK 21 and JDK 25 and prints both (differences marked
  `[JDK25: …]`).
* Probe files `pA.py` (literals/escapes), `pB.py`/`pB2.py` (classes), `pP.py`/`pP2.py`
  (property names), `pD.py`/`pD2.py`/`pD3.py` (anchors, `\b`, `\R`, `\X`),
  `pQ*.py` (quantifiers, zero-length loops), `pG.py` (group/flag/lookbehind syntax),
  `pL.py`/`pL2.py`/`pW*.py` (lookbehind), `pR.py` (back-references), `pK.py` (capture
  commitment), `pF.py`/`pX.py` (flags, COMMENTS), `pCI.py` (case-insensitivity),
  `pCE.py` (CANON_EQ), `pU.py` (surrogates, atomic), `pS.py`/`pS2.py` (stack), `pZ.py` (`&`-runs, branch reset). Outputs:
  `out_<file>.txt` (3 153 probes, ids unique across files).
* Full-range checkers (public API only): `PropCheck.java` (predefined/POSIX/property
  sets vs `Character` predicates; CI hypotheses), `NameCheck.java` (3 016 property-name
  spellings), `ForNameCheck.java` + `fornames.txt` (3 948 block/script spellings vs
  `forName`), `ScriptList.java`, `BlockAliases.java`, `SetDump.java` (sets vs UCD files),
  `CaseCheck.java`/`CaseDump.java` (case-insensitive pairs, literal/class/range/backref),
  `MapDump.java` (case maps vs UnicodeData), `WordCheck.java` (`\b` rules, all code
  points), `GraphemeCheck.java` (UAX #29 test file), `uax29.py` + `gen_graph.py` +
  `graph_cmp.py` (independent UAX #29 reference, 10 000 random cases), `Msg.java`
  (message format).

## Appendix C. Error descriptions observed

`Illegal/unsupported escape sequence` — unknown `\letter`, stray `\E`, disallowed
escapes in classes (index = letter).
`Illegal control escape sequence` — `\c` at end.
`Illegal octal escape sequence` — `\0`, `\08`.
`Illegal hexadecimal escape sequence` — `\x`, `\xg1`, `\x4`, `\x{}`.
`Unclosed hexadecimal escape sequence` — `\x{41`, `\x{4G}`.
`Hexadecimal codepoint is too big` — `\x{110000}`.
`Illegal Unicode escape sequence` — `\u041`, `\u004G`, `\uu0041`, `\u` at end.
`Illegal character name escape sequence` — `\N` without `{`.
`Unclosed character name escape sequence` — `\N{…` without `}`.
`Unknown character name [name]` — unresolvable `\N{}`.
`Unescaped trailing backslash`.
`\k is not followed by '<' for named capturing group` (the backslash is shown as
U+005C).
`Unclosed character class` — missing `]`, `[]`, `[^]`, `[[]`, `[\Qab]`.
`Illegal character range` — `[z-a]`, `[a-`, `[a-\d]`, `[a--]`, `[a-\]]`, `[a-&&b]`.
`Bad class syntax` — `[&&]`.
`Unknown character property name {name}`; `Unknown Unicode property {name=<k>,
value=<v>}`; `Empty character family`; `Unclosed character family`.
`Illegal repetition`; `Unclosed counted closure`; `Illegal repetition range`;
`Dangling meta character 'c'`.
`Unclosed group`; `Unmatched closing ')'`; `Unknown inline modifier`.
`capturing group name does not start with a Latin letter`;
`named capturing group is missing trailing '>'`;
`Named capturing group <n> is already defined`;
`named capturing group <n> does not exist`.
`Look-behind group does not have an obvious maximum length`.
`Stack overflow during pattern compilation`.
`IllegalArgumentException` (not PSE) for undefined bits in the `flags` argument
(Javadoc; not reachable from Cypher).

## Appendix D. Rule index

ERR-1…3, LIT-1…11, CLS-1…11, PRE-1…5, PROP-1…9, SCR-1, BLK-1, ANC-1…7, WB-1…4,
LBRK-1, GC-1…3, QNT-1…8, GRP-1…3, FLG-1…6, XM-1…2, LA-1, LB-1…6, BR-1…8, CAP-1…5,
CI-1…6, SUR-1…4, CEQ-1, RES-1…3, J25-1.
