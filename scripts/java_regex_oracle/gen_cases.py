#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
r"""Seeded random `=~` cases for the Java regex oracle.

Writes `pattern<TAB>subject` lines in the oracle's escape format (`\\`,
`\t`, `\n`, `\r`, `\x{HEX}`) for `JavaRegexOracle.java` to answer:

    gen_cases.py --seed 1 --patterns 5000 --profile all \
      | java scripts/java_regex_oracle/JavaRegexOracle.java > cases.tsv
    UNI_JAVA_REGEX_ORACLE=cases.tsv cargo nextest run -p uni-query-functions \
      --lib -E 'test(answers_match_java)'

Profiles:
  anchors  anchors, line terminators, word boundaries and combining marks
           over short subjects (the inputs the automaton guards are about).
  all      every construct the translation handles: look-around (look-behind
           included), backreferences, atomic groups, possessive and lazy
           quantifiers, Java-only syntax, classes, properties and flags, plus
           mutated patterns that Java may reject.
  unicode  one-character subjects across every plane against properties,
           scripts, Unicode classes and word boundaries, and case-insensitive
           literals against their case partners: the inputs whose answers
           depend on the Unicode version of the reference JDK.
  lookbehind
           look-behinds whose bodies read supplementary characters (fixed,
           bounded and unbounded windows, alternations, groups), over
           subjects dense in surrogate pairs.
  backref  backreferences before, after and inside the groups they name,
           through repetitions of every kind.
  javaprops
           `\p{java…}` (the `Character.isXxx` predicates) and Unicode blocks
           (`\p{In…}`, `\p{blk=…}`) in every spelling, over code points
           across every plane.
  names    `\N{…}`: real names, case and whitespace variants, block-and-hex
           names, near misses, in and out of classes.
  grapheme `\X` and `\b{g}` over subjects of grapheme-cluster material:
           CR LF, controls, marks, ZWJ emoji sequences, regional indicators,
           Hangul jamo and syllables, prepend and spacing marks.
  canon    `(?c)` classes and properties over precomposed and decomposed
           forms, reordered marks and composition exclusions.
  cibackref
           `(?i)` and `(?iu)` backreferences over case partners (Kelvin sign,
           long s, sharp s, sigma, title case, Deseret, dotted and dotless i).
  emptyloop
           repetitions whose body can match empty depending on position, and
           backreferences into repetitions that can match empty.
  lookaround
           look-behinds and look-aheads whose bodies hold word boundaries,
           look-arounds, backreferences, `\X` and `\b{g}`, and look-behind
           windows whose length overflows Java's `int` arithmetic.

The block names and character names come from the tables generated from
the pinned JDK (`crates/uni-query-functions/src/java_regex/ucd_tables.rs`
and `ucd_names.bin`).

The answers are recorded with the pinned JDK (Java 21) and checked against
it; `--profile` sizes of 300k cases or more are what the regex gate runs.
"""

import argparse
import itertools
import os
import random
import re
import struct

TABLES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "crates",
                      "uni-query-functions", "src", "java_regex")


def esc(s):
    out = []
    for c in s:
        if c == "\\":
            out.append("\\\\")
        elif c == "\t":
            out.append("\\t")
        elif c == "\n":
            out.append("\\n")
        elif c == "\r":
            out.append("\\r")
        elif ord(c) < 32 or ord(c) > 126:
            out.append("\\x{%x}" % ord(c))
        else:
            out.append(c)
    return "".join(out)


# ---------------------------------------------------------------------------
# anchors profile
# ---------------------------------------------------------------------------

ANCHOR_ATOMS = [
    "a", "\\x{e9}", ".", "\\n", "\\r", "\\x{85}", "\\x{2028}", "$", "^", "\\b", "\\B",
    "\\x{301}", "(?s:.)*", ".*", "\\s*", "\\Z", "\\R", "\\z", "\\A", "\\G", "\\w",
    "\\W", "(?d:$)", "(?d:^)", "(?d:.)", "(?m:$)", "(?m:^)", "\\x{1F600}",
]
ANCHOR_SUBJECT = ["a", "\n", "\r", "\x85", " ", " ", "́", "é", "\U0001F600", "ж"]
ANCHOR_FLAGS = ["", "(?m)", "(?U)", "(?mU)", "(?d)", "(?md)", "(?s)"]


def anchors(rng, n_patterns, per):
    subs = [""] + ["".join(t) for k in (1, 2, 3) for t in itertools.product(ANCHOR_SUBJECT, repeat=k)]
    for _ in range(n_patterns):
        p = rng.choice(ANCHOR_FLAGS) + "".join(rng.choice(ANCHOR_ATOMS) for _ in range(rng.randint(1, 4)))
        for s in rng.sample(subs, per):
            yield p, s


# ---------------------------------------------------------------------------
# all profile
# ---------------------------------------------------------------------------

SUBJECT_CHARS = [
    "a", "b", "A", "B", "k", "K", "s", "S", "\n", "\r", "\x85", " ", "\t", "é", "́",
    "_", "1", "\U0001F600", "\U0001D400", "K", "ſ", "İ", "ı", " ",
    "\x1b", "\x01", "-", "&", "~", "ß", "\U00010400", "\U00010428", "ж",
]

BASE_ATOMS = [
    "a", "b", "A", "k", "s", ".", "\\w", "\\W", "\\d", "\\D", "\\s", "\\S", "[ab]", "[^a]",
    "\\n", "\\r", "\\x{85}", "\\x{2028}", "\\x{301}", "\\x{e9}", "_", "(?s:.)", "\\x{1F600}",
    "\\x{1D400}", "\\h", "\\H", "\\v", "\\V", "\\e", "\\cA", "\\0101", "\\0141", "\\u0061",
    "\\uD83D\\uDE00", "\\x41", "\\Qa.\\E", "\\Q*\\E", "\\Q\\E", "\\t", "\\a", "\\f",
    "[a-z]", "[A-Z&&[^K]]", "[a~~b]", "[\\w&&[^_]]", "[a[b]]", "[^a[b]]", "[ab&&b]",
    "[\\x{1F600}-\\x{1F601}]", "[\\x{e9}-\\x{ff}]", "[-a]", "[a-]", "[]a]", "[\\Q]\\E]",
    "\\p{L}", "\\p{Lu}", "\\P{L}", "\\p{IsLatin}", "\\p{Alpha}", "\\p{IsAlphabetic}",
    "\\p{javaLowerCase}", "\\pL", "\\p{sc=Grek}", "\\p{gc=Lu}", "\\p{Lower}", "\\p{Punct}",
    "\\x{212a}", "\\x{17f}", "\\x{df}", "\\x{10400}", "\\x{10428}", "\\x{436}", "\\<", "\\>",
    "\\-", "~", "&", "x{1}{2}", "a{2}{1}",
]
ANCHORS = ["^", "$", "\\b", "\\B", "\\A", "\\z", "\\Z", "\\G", "\\R"]
OPEN = ["(?=", "(?!", "(?<=", "(?<!", "(?>", "(", "(?:", "(?<g{n}>", "(?i:", "(?-i:", "(?x:", "(?d:", "(?m:", "(?u:"]
FLAGS = ["", "", "(?m)", "(?s)", "(?ms)", "(?U)", "(?i)", "(?iu)", "(?d)", "(?x)", "(?)", "(?i-i)", "(?iU)"]
QUANTS = ["*", "+", "?", "{1,2}", "{0,2}", "{2}", "{0}", "{1,}", "{0,1}"]


# Strings each base atom can match, for subjects that exercise matches.
SAMPLES = {
    "a": ["a"], "b": ["b"], "A": ["A", "a"], "k": ["k", "K", "\u212a"], "s": ["s", "S", "\u017f"],
    ".": ["a", "\U0001F600", "\n"], "\\w": ["a", "_", "1", "\u00e9"], "\\W": [" ", "\U0001F600", "\u00e9"],
    "\\d": ["1"], "\\D": ["a"], "\\s": [" ", "\n"], "\\S": ["a"], "[ab]": ["a", "b"], "[^a]": ["b", "\U0001F600"],
    "\\n": ["\n"], "\\r": ["\r"], "\\x{85}": ["\x85"], "\\x{2028}": ["\u2028"], "\\x{301}": ["\u0301"],
    "\\x{e9}": ["\u00e9", "\u00c9"], "_": ["_"], "(?s:.)": ["\n", "a"], "\\x{1F600}": ["\U0001F600"],
    "\\x{1D400}": ["\U0001D400"], "\\h": [" ", "\t"], "\\H": ["a"], "\\v": ["\n", "\x85"], "\\V": ["a"],
    "\\e": ["\x1b"], "\\cA": ["\x01"], "\\0101": ["A"], "\\0141": ["a"], "\\u0061": ["a"],
    "\\uD83D\\uDE00": ["\U0001F600"], "\\x41": ["A"], "\\Qa.\\E": ["a."], "\\Q*\\E": ["*"], "\\Q\\E": [""],
    "\\t": ["\t"], "[a-z]": ["a", "k", "\u212a"], "[A-Z&&[^K]]": ["A", "S", "k"], "[a~~b]": ["~", "a"],
    "\\p{L}": ["a", "\U0001D400", "\u0436"], "\\p{Lu}": ["A", "\U00010400"], "\\P{L}": ["1", "\U0001F600"],
    "\\x{212a}": ["\u212a", "k"], "\\x{17f}": ["\u017f", "s"], "\\x{df}": ["\u00df"], "\\x{10400}": ["\U00010400", "\U00010428"],
    "\\<": ["<"], "\\>": [">"], "\\-": ["-"], "~": ["~"], "&": ["&"], "x{1}{2}": ["x"], "a{2}{1}": ["aa"],
}
ANCHOR_SAMPLES = {"^": ["", "\n"], "$": ["", "\n"], "\\b": [""], "\\B": [""], "\\A": [""], "\\z": [""],
                  "\\Z": ["", "\n"], "\\G": [""], "\\R": ["\r\n", "\n", "\r"]}


class Gen:
    """Builds a pattern and, beside it, a string it is likely to match."""

    def __init__(self, rng):
        self.rng = rng
        self.groups = 0
        self.named = []

    def pick(self, options):
        return self.rng.choice(options)

    def base(self):
        a = self.pick(BASE_ATOMS)
        return a, (lambda: self.pick(SAMPLES.get(a, SUBJECT_CHARS)))

    def atom(self, depth):
        r = self.rng.random()
        if r < 0.45:
            return self.base()
        if r < 0.60:
            a = self.pick(ANCHORS)
            return a, (lambda: self.pick(ANCHOR_SAMPLES[a]))
        if r < 0.68 and self.groups > 0:
            if self.named and self.rng.random() < 0.3:
                return "\\k<%s>" % self.pick(self.named), (lambda: self.pick(["", "a"]))
            return "\\%d" % self.rng.randint(1, self.groups + 1), (lambda: self.pick(["", "a"]))
        if depth < 2 and self.rng.random() < 0.6:
            op = self.pick(OPEN)
            if op.startswith("(?<g"):
                self.groups += 1
                name = "g%d" % self.groups
                self.named.append(name)
                op = "(?<%s>" % name
            elif op == "(":
                self.groups += 1
            body, sample = self.seq(depth + 1)
            if op in ("(?=", "(?!", "(?<=", "(?<!"):
                return op + body + ")", (lambda: "")
            return op + body + ")", sample
        return self.base()

    def quant(self, a, sample):
        if self.rng.random() < 0.6:
            return a, sample
        q = self.pick(QUANTS)
        lo, hi = {"*": (0, 3), "+": (1, 3), "?": (0, 1), "{1,2}": (1, 2), "{0,2}": (0, 2), "{2}": (2, 2),
                  "{0}": (0, 0), "{1,}": (1, 3), "{0,1}": (0, 1)}[q]
        return (a + q + self.pick(["", "", "?", "+"]),
                lambda: "".join(sample() for _ in range(self.rng.randint(lo, hi))))

    def seq(self, depth):
        parts = []
        for _ in range(self.rng.randint(1, 4)):
            a, sample = self.atom(depth)
            if a not in ANCHORS or self.rng.random() < 0.1:
                a, sample = self.quant(a, sample)
            parts.append((a, sample))
        s = "".join(a for a, _ in parts)
        samplers = [smp for _, smp in parts]
        sample = lambda: "".join(f() for f in samplers)
        if self.rng.random() < 0.15:
            alt = [self.quant(*self.atom(depth)) for _ in range(self.rng.randint(0, 2))]
            s += "|" + "".join(a for a, _ in alt)
            left = sample
            sample = lambda: left() if self.rng.random() < 0.5 else "".join(f() for _, f in alt)
        if "(?x" in s or self.rng.random() < 0.03:
            s = s.replace("a", "a ", 1) + (" # c\n" if self.rng.random() < 0.3 else "")
        return s, sample


def mutate(rng, p):
    """A small edit that Java may well reject."""
    if not p:
        return p
    i = rng.randrange(len(p))
    op = rng.randrange(3)
    if op == 0:
        return p[:i] + p[i + 1:]
    junk = rng.choice(["(", ")", "[", "]", "{", "}", "\\", "*", "+", "?", "{,2}", "&&", "-", "\\k<x>", "(?<", "\\p{", "\\x{"])
    if op == 1:
        return p[:i] + junk + p[i:]
    return p[:i] + junk + p[i + 1:]


def all_constructs(rng, n_patterns, per):
    for _ in range(n_patterns):
        g = Gen(rng)
        body, sample = g.seq(0)
        p = rng.choice(FLAGS) + body
        if rng.random() < 0.08:
            p = mutate(rng, p)
        for k in range(per):
            if k % 2 == 0:
                s = sample()
                # Half the samples get a small edit, to probe near misses.
                if k % 4 == 2 and s:
                    i = rng.randrange(len(s))
                    s = s[:i] + rng.choice(SUBJECT_CHARS + [""]) + s[i + 1:]
            else:
                s = "".join(rng.choice(SUBJECT_CHARS) for _ in range(rng.randint(0, 6)))
            yield p, s


# ---------------------------------------------------------------------------
# unicode profile
# ---------------------------------------------------------------------------

UNICODE_PROPS = [
    "\\p{L}", "\\p{Lu}", "\\p{Ll}", "\\p{Lt}", "\\p{Lm}", "\\p{Lo}", "\\p{M}", "\\p{Mn}", "\\p{Mc}",
    "\\p{Me}", "\\p{N}", "\\p{Nd}", "\\p{Nl}", "\\p{No}", "\\p{P}", "\\p{S}", "\\p{So}", "\\p{Sm}",
    "\\p{Sk}", "\\p{Sc}", "\\p{Z}", "\\p{C}", "\\p{Cn}", "\\p{Cf}", "\\p{Co}", "\\p{LC}", "\\p{LD}",
    "\\p{IsAlphabetic}", "\\p{IsIdeographic}", "\\p{IsLetter}", "\\p{IsLowercase}", "\\p{IsUppercase}",
    "\\p{IsTitlecase}", "\\p{IsPunctuation}", "\\p{IsDigit}", "\\p{IsHex_Digit}", "\\p{IsAssigned}",
    "\\p{IsEmoji}", "\\p{IsEmoji_Presentation}", "\\p{IsEmoji_Modifier_Base}", "\\p{IsEmoji_Component}",
    "\\p{IsExtended_Pictographic}", "\\p{IsWhite_Space}", "\\p{IsNoncharacter_Code_Point}",
    "\\p{IsWord}", "(?i)\\p{IsLowercase}", "(?i)\\p{Lu}", "(?i)\\p{IsTitlecase}",
    "(?U)\\w", "(?U)\\W", "(?U)\\d", "(?U)\\s", "(?U)\\p{Alpha}", "(?U)\\p{Alnum}", "(?U)\\p{Graph}",
    "(?U)\\p{Print}", "(?U)\\p{Punct}", "(?U)\\p{Lower}", "(?U)\\p{Upper}", "(?iU)\\p{Lower}",
    "(?U)\\b.\\b", "(?U)\\B.", "(?U)a\\b.", "(?U).\\Ba", "\\b.", ".\\b", "a\\b.",
    "\\p{IsUnknown}", "\\p{IsCommon}", "\\p{IsInherited}", "\\p{IsLatin}", "\\p{IsGreek}",
    "\\p{IsCyrillic}", "\\p{IsArabic}", "\\p{IsHan}", "\\p{IsKawi}", "\\p{IsNag_Mundari}",
    "\\p{sc=Zzzz}",
]
# Script names around the Unicode 15.0 / 16.0 boundary; the oracle decides
# which compile.
UNICODE_SCRIPT_NAMES = [
    "Garay", "Gara", "Gurung_Khema", "Gukh", "Kirat_Rai", "Krai", "Ol_Onal", "Onao", "Sunuwar",
    "Sunu", "Todhri", "Todr", "Tulu_Tigalari", "Tutg", "Kawi", "Nag_Mundari", "Nagm", "Cypro_Minoan",
    "Old_Uyghur", "Tangsa", "Toto", "Vithkuqi", "Latin", "Unknown",
]


def unicode_props(rng, n_patterns, per):
    """Single code points against properties and case partners."""
    hot = [(0x0370, 0x03FF), (0x0890, 0x089F), (0x1C80, 0x1C8F), (0xA7C0, 0xA7FF), (0x10D40, 0x10D8F),
           (0x10EC0, 0x10EFF), (0x11380, 0x113FF), (0x116D0, 0x116FF), (0x11F00, 0x11F5F), (0x16100, 0x1613F),
           (0x16D40, 0x16D7F), (0x1E5D0, 0x1E5FF), (0x1F000, 0x1FBFF), (0x2EBF0, 0x2EE5F), (0x31350, 0x323AF),
           (0x0300, 0x036F), (0x1E900, 0x1E95F), (0x10400, 0x1044F), (0x1F00, 0x1FFF), (0xAB70, 0xABBF)]

    def cp():
        if rng.random() < 0.6:
            a, b = rng.choice(hot)
            c = rng.randint(a, b)
        else:
            c = rng.randint(0, 0x10FFFF)
        return c if not 0xD800 <= c <= 0xDFFF else 0x41

    for _ in range(n_patterns):
        c = cp()
        r = rng.random()
        if r < 0.35:
            partners = {c, ord(chr(c).upper()[0]), ord(chr(c).lower()[0]), cp()}
            lit = "\\x{%x}" % c
            for p in ["(?iu)" + lit, "(?iu)[" + lit + "]", "(?iu)[" + lit + "-" + lit + "]", "(?i)" + lit]:
                for s in sorted(partners)[:per]:
                    yield p, chr(s)
                    yield "(?iu)\\x{%x}" % s, chr(c)
        elif r < 0.45:
            name = rng.choice(UNICODE_SCRIPT_NAMES)
            for p in ["\\p{Is%s}" % name, "\\p{sc=%s}" % name, "\\P{Is%s}" % name]:
                for _ in range(max(1, per // 2)):
                    yield p, chr(cp())
        else:
            for p in rng.sample(UNICODE_PROPS, min(per, len(UNICODE_PROPS))):
                s = chr(c) if "." not in p and "a" not in p.replace("Alpha", "").replace("\\p{IsArabic}", "") \
                    else rng.choice(["", "a", " "]) + chr(c) + rng.choice(["", "a", " "])
                yield p, s


# ---------------------------------------------------------------------------
# lookbehind profile
# ---------------------------------------------------------------------------

LB_ATOMS = [
    "\\p{L}", "\\P{L}", "[^a]", ".", "(?s:.)", "\\W", "\\S", "\\D", "\\H", "\\V", "\\p{Lu}", "\\p{Ll}",
    "\\p{IsAlphabetic}", "[\\x{10000}-\\x{1FFFF}]", "[a\\x{10400}-\\x{1044F}]", "\\p{So}", "\\P{So}",
    "[^\\x{10428}]", "\\p{IsLatin}", "\\p{IsDeseret}", "(?iu:\\p{Lu})", "(?i:[a-z])", "\\w", "\\s",
    "a", "\\x{e9}", "[ab]", "1", "\\p{Cs}", "[\\x{D800}-\\x{DFFF}]", "[\\x{DC00}-\\x{DFFF}a]", "\\x{436}",
    "\\x{1F600}", "\\x{10428}", "(?iu:\\x{10400})", "\\uD83D\\uDE00",
]
LB_QUANTS = ["", "", "", "", "?", "??", "{2}", "{1,2}", "{0,2}", "{1,3}", "{0,3}?", "{2,3}", "{0,1}", "*", "+", "?+"]
LB_SUBJECT = ["a", "b", "é", "\U0001F600", "\U00010428", "\U00010400", "\U0001D400", " ", "1", "ж", "\n"]


def lb_body(rng, depth=0):
    parts = []
    for _ in range(rng.randint(1, 3)):
        r = rng.random()
        if r < 0.15 and depth < 1:
            a = rng.choice(["(?:", "(?:", "("]) + lb_body(rng, depth + 1) + ")"
        elif r < 0.19:
            a = rng.choice(["\\b", "(?=\\p{L})", "\\B"])
        else:
            a = rng.choice(LB_ATOMS)
        parts.append(a + (rng.choice(LB_QUANTS) if not a.startswith(("\\b", "\\B", "(?=")) else ""))
    body = "".join(parts)
    if rng.random() < 0.25:
        body += "|" + "".join(rng.choice(LB_ATOMS) + rng.choice(LB_QUANTS[:8]) for _ in range(rng.randint(0, 2)))
    return body


def lookbehind(rng, n_patterns, per):
    subs = [""] + ["".join(t) for k in (1, 2, 3) for t in itertools.product(LB_SUBJECT, repeat=k)]
    for _ in range(n_patterns):
        look = rng.choice(["(?<=", "(?<!"]) + lb_body(rng) + ")"
        pre = rng.choice(["(?s).*", "(?s).*?", "(?s)(.*)", ".{0,2}", "\\p{L}*", "a?", "", "(?s).*\\x{10428}?"])
        post = rng.choice(["(?s).*", "", "(?s)(.*)", "\\p{L}?", ".{0,3}", "\\P{L}*"])
        p = rng.choice(["", "", "(?u)", "(?i)", "(?U)", "(?iu)"]) + pre + look + post
        if rng.random() < 0.2:
            p = pre + look + rng.choice(["a", "\\x{e9}", "."]) + rng.choice(["(?<=", "(?<!"]) + lb_body(rng) + ")" + post
        for s in rng.sample(subs, per):
            yield p, s


# ---------------------------------------------------------------------------
# backref profile
# ---------------------------------------------------------------------------

BR_ATOMS = ["a", "b", "\\p{L}", ".", "(?s:.)", "[ab]", "\\x{10428}", "\\x{e9}", "\\w", "a?", "b*"]
BR_QUANTS = ["", "", "?", "??", "*", "*?", "+", "+?", "{0,2}", "{0,2}?", "{2}", "{1,2}", "?+", "*+", "{0,2}+"]
BR_SUBJECT = ["a", "b", "aa", "ab", "\U00010428", "é", "", "ba"]


class BrGen:
    def __init__(self, rng):
        self.rng = rng
        self.groups = 0
        self.open = []

    def ref(self):
        k = self.rng.randint(1, self.groups + 1)
        return "\\%d" % k if self.rng.random() < 0.85 else "\\k<g%d>" % k

    def seq(self, depth):
        parts = []
        for _ in range(self.rng.randint(1, 3)):
            r = self.rng.random()
            if r < 0.35 and depth < 3:
                self.groups += 1
                k = self.groups
                head = "(" if self.rng.random() < 0.7 else "(?<g%d>" % k
                body = self.seq(depth + 1)
                if self.rng.random() < 0.3:
                    body += "|" + self.seq(depth + 1)
                a = head + body + ")"
            elif r < 0.6 and self.groups > 0:
                a = self.ref()
            else:
                a = self.rng.choice(BR_ATOMS)
            parts.append(a + self.rng.choice(BR_QUANTS))
        return "".join(parts)


def backref(rng, n_patterns, per):
    subs = [""] + ["".join(t) for k in (1, 2, 3, 4) for t in itertools.product(BR_SUBJECT[:6], repeat=k)]
    for _ in range(n_patterns):
        g = BrGen(rng)
        p = rng.choice(["", "", "(?s)", "(?u)", "(?x)"]) + g.seq(0)
        for s in rng.sample(subs, per):
            yield p, s


# ---------------------------------------------------------------------------
# data from the generated tables
# ---------------------------------------------------------------------------

_CACHE = {}


def block_data():
    """(keys, ranges): every `UnicodeBlock.forName` key with its block index,
    and the `UnicodeBlock.of` ranges."""
    if "blocks" not in _CACHE:
        text = open(os.path.join(TABLES, "ucd_tables.rs"), encoding="utf-8").read()
        keys_src = text[text.index("pub(crate) const BLOCK_KEYS"):]
        keys_src = keys_src[:keys_src.index("];")]
        keys = [(k, int(v)) for k, v in re.findall(r'\("([^"]*)", (\d+)\)', keys_src)]
        ranges_src = text[text.index("pub(crate) const BLOCK: "):]
        ranges_src = ranges_src[:ranges_src.index("];")]
        ranges = [(int(a, 16), int(b, 16), int(k))
                  for a, b, k in re.findall(r"\(0x([0-9A-F]+), 0x([0-9A-F]+), (\d+)\)", ranges_src)]
        _CACHE["blocks"] = (keys, ranges)
    return _CACHE["blocks"]


def names_data():
    """Every `CharacterName` name with its code point, from `ucd_names.bin`."""
    if "names" not in _CACHE:
        data = open(os.path.join(TABLES, "ucd_names.bin"), "rb").read()
        at = 0
        (nwords,) = struct.unpack_from("<I", data, at)
        at += 4
        words = []
        for _ in range(nwords):
            n = data[at]
            words.append(data[at + 1:at + 1 + n].decode("ascii"))
            at += 1 + n
        (nnames,) = struct.unpack_from("<I", data, at)
        at += 4
        out = []
        for _ in range(nnames):
            cp = data[at] | data[at + 1] << 8 | data[at + 2] << 16
            k = data[at + 3]
            at += 4
            parts = []
            for t in range(k):
                (tok,) = struct.unpack_from("<H", data, at)
                at += 2
                if t:
                    parts.append("-" if tok & 0x8000 else " ")
                parts.append(words[tok & 0x7FFF])
            out.append((cp, "".join(parts)))
        _CACHE["names"] = out
    return _CACHE["names"]


def safe_chr(c):
    """A subject character: surrogates cannot be in a Cypher string."""
    return chr(c) if not 0xD800 <= c <= 0xDFFF else "a"


def random_cp(rng, hot=()):
    if hot and rng.random() < 0.6:
        a, b = rng.choice(hot)
        return rng.randint(a, b)
    r = rng.random()
    if r < 0.3:
        return rng.randint(0, 0x2FF)
    if r < 0.7:
        return rng.randint(0, 0xFFFF)
    return rng.randint(0x10000, 0x10FFFF)


# ---------------------------------------------------------------------------
# javaprops profile
# ---------------------------------------------------------------------------

JAVA_PROPS = ["javaLowerCase", "javaUpperCase", "javaAlphabetic", "javaIdeographic", "javaTitleCase",
              "javaDigit", "javaDefined", "javaLetter", "javaLetterOrDigit", "javaJavaIdentifierStart",
              "javaJavaIdentifierPart", "javaUnicodeIdentifierStart", "javaUnicodeIdentifierPart",
              "javaIdentifierIgnorable", "javaSpaceChar", "javaWhitespace", "javaISOControl", "javaMirrored"]


def spellings(name, rng):
    """Spellings of a block name that `forName` may or may not accept."""
    out = [name, name.lower(), name.title(), name.replace(" ", "_"), name.replace(" ", ""),
           name.replace("_", " "), name.replace("-", "_"), " " + name, name + "x"]
    if "I" in name:
        out.append(name.replace("I", "\u0131", 1))
    if "S" in name:
        out.append(name.lower().replace("s", "\u017f", 1))
    return [rng.choice(out)]


def javaprops(rng, n_patterns, per):
    keys, ranges = block_data()
    for _ in range(n_patterns):
        r = rng.random()
        if r < 0.5:
            name = rng.choice(JAVA_PROPS)
            if rng.random() < 0.1:
                name = name[:4] + name[4:].lower()
            form = rng.choice(["\\p{%s}", "\\P{%s}", "\\p{Is%s}", "\\p{gc=%s}", "[\\p{%s}]", "[^\\p{%s}]",
                               "[\\p{%s}&&\\p{L}]", "[\\p{%s}a-c]"]) % name
            p = rng.choice(["", "", "(?i)", "(?iu)", "(?U)"]) + form
            for _ in range(per):
                yield p, safe_chr(random_cp(rng, [(0, 0x24F), (0x2000, 0x206F), (0x1D400, 0x1D7FF)]))
        else:
            key, k = rng.choice(keys)
            name = spellings(key, rng)[0]
            form = rng.choice(["\\p{In%s}", "\\P{In%s}", "\\p{blk=%s}", "\\p{block=%s}", "\\p{Block=%s}",
                               "[\\p{In%s}]", "\\p{in%s}"]) % name
            p = rng.choice(["", "", "(?i)"]) + form
            inside = [(a, b) for a, b, kk in ranges if kk == k]
            for _ in range(per):
                if inside and rng.random() < 0.6:
                    a, b = rng.choice(inside)
                    yield p, safe_chr(rng.randint(a, b))
                else:
                    yield p, safe_chr(random_cp(rng))


# ---------------------------------------------------------------------------
# names profile
# ---------------------------------------------------------------------------

FALLBACK_BLOCKS = [("CJK UNIFIED IDEOGRAPHS", 0x4E00, 0x9FFF), ("HANGUL SYLLABLES", 0xAC00, 0xD7A3),
                   ("PRIVATE USE AREA", 0xE000, 0xF8FF), ("TANGUT", 0x17000, 0x187F7),
                   ("CJK UNIFIED IDEOGRAPHS EXTENSION B", 0x20000, 0x2A6DF),
                   ("SUPPLEMENTARY PRIVATE USE AREA-A", 0xF0000, 0xFFFFD), ("BASIC LATIN", 0x0, 0x7F)]


def name_variant(rng, name):
    r = rng.random()
    if r < 0.4:
        return name
    if r < 0.55:
        return name.lower()
    if r < 0.65:
        return rng.choice([" ", "\t", "  "]) + name + rng.choice(["", " ", "\t"])
    if r < 0.7 and "I" in name:
        return name.lower().replace("i", "\u0131", 1)
    if r < 0.75 and "SS" in name:
        return name.replace("SS", "\u00df", 1)
    if r < 0.85:
        i = rng.randrange(len(name))
        return name[:i] + name[i + 1:]
    return name + rng.choice([" ", "X", "-"]) if rng.random() < 0.5 else name.replace(" ", "  ", 1)


def names(rng, n_patterns, per):
    named = names_data()
    for _ in range(n_patterns):
        if rng.random() < 0.75:
            cp, name = rng.choice(named)
        else:
            block, a, b = rng.choice(FALLBACK_BLOCKS)
            cp = rng.randint(a, b)
            hexs = rng.choice(["%X" % cp, "%X" % cp, "%x" % cp, "0%X" % cp, "+%X" % cp])
            name = block + " " + hexs
        n = name_variant(rng, name).replace("\\", "\\\\").replace("}", "")
        esc_n = "\\N{" + n + "}"
        form = rng.choice(["%s", "%s", "[%s]", "[^%s]", "%s+", "[a%s]", "(?i)%s", "(?x)%s", "%s?a"]) % esc_n
        if rng.random() < 0.1:
            cp2, name2 = rng.choice(named)
            lo, hi = sorted([(cp, name), (cp2, name2)])
            form = "[\\N{%s}-\\N{%s}]" % (lo[1], hi[1])
        subs = [safe_chr(cp), safe_chr(cp) * 2, safe_chr(cp) + "a", safe_chr(random_cp(rng)),
                safe_chr(min(cp + 1, 0x10FFFF)), "a", ""]
        for s in rng.sample(subs, min(per, len(subs))):
            yield form, s


# ---------------------------------------------------------------------------
# grapheme profile
# ---------------------------------------------------------------------------

GR_SUBJECT = ["a", "b", "\r", "\n", "\x01", "\u0301", "\u0308", "\u200d", "\u200c", "\U0001F468", "\U0001F469",
              "\U0001F3FB", "\U0001F1FA", "\U0001F1F8", "\u1100", "\u1161", "\u11a8", "\uac00", "\uac01",
              "\u0600", "\u0903", "\u0e33", "\u102b", "\u0378", "\u2028", "\U0001F600", "\u00e9", " ",
              "\U00010428", "\uff9e", "\U000e0020", "\u0d4e", "\u2764", "\ufe0f",
              # Indic conjuncts: Unicode 15.1 (Java 22+) joins consonant,
              # virama, consonant into one cluster (GB9c); Java 21 does not.
              "\u0915", "\u094d", "\u0937", "\u0924"]
GR_ATOMS = ["\\X", "\\X", "\\b{g}", "\\b{g}", ".", "(?s:.)", "\\p{M}", "\\x{301}", "a", "\\R", "\\P{M}",
            "(?=\\X)", "(?<=\\X)", "(?=\\b{g})", "\\x{1F468}", "\\x{200d}", "[\\x{1100}-\\x{11ff}]", "\\b",
            "(\\X)", "\\1"]
GR_QUANTS = ["", "", "", "*", "+", "?", "{2}", "{0,2}", "*?", "+?", "*+", "{1,3}"]


def grapheme(rng, n_patterns, per):
    for _ in range(n_patterns):
        parts = []
        groups = 0
        for _ in range(rng.randint(1, 4)):
            a = rng.choice(GR_ATOMS)
            if a == "(\\X)":
                groups += 1
            if a == "\\1" and groups == 0:
                a = "\\X"
            q = rng.choice(GR_QUANTS) if not a.startswith(("(?=", "(?<=", "\\b")) else ""
            parts.append(a + q)
        p = rng.choice(["", "", "(?s)", "(?U)", "(?i)", "(?c)"]) + "".join(parts)
        if rng.random() < 0.1:
            p = "(?=" + rng.choice(["a", "..", "\\X"]) + ")" + p
        for _ in range(per):
            yield p, "".join(rng.choice(GR_SUBJECT) for _ in range(rng.randint(0, 5)))


# ---------------------------------------------------------------------------
# canon profile
# ---------------------------------------------------------------------------

CANON_SUBJECT = ["e", "\u00e9", "e\u0301", "A", "\u00c5", "A\u030a", "\u212b", "\u2126", "\u03a9", "a\u0323\u0302",
                 "a\u0302\u0323", "\u1ead", "\u0958", "\u0915\u093c", "\u1100\u1161", "\uac00", "\uac00\u11a8",
                 "\u1100\u1161\u11a8", "\uac01", "o\u0308\u0304", "\u022b", "x", "\u0301", "\u1e31", "k\u0301",
                 "\U0001109a", "\U00011099\U000110ba", "a", "\U0001D15E", "\U0001D157\U0001D165"]
CANON_SETS = ["[\\x{e9}]", "[e]", "[\\x{c5}]", "\\p{L}", "\\p{Ll}", "\\P{M}", "[^a]", "[a-z]", "[\\x{ac00}-\\x{d7a3}]",
              "\\p{IsHangul}", "[\\x{1ead}]", "[\\x{3a9}]", "[\\x{2126}]", "[\\x{958}]", "\\w", "[\\x{22b}]",
              "[\\x{1109a}]", "[\\x{1d15e}]", "\\p{InGreek}", "[\\p{L}&&[^e]]"]


def canon(rng, n_patterns, per):
    for _ in range(n_patterns):
        parts = []
        for _ in range(rng.randint(1, 3)):
            a = rng.choice(CANON_SETS + ["\\x{301}", "a", ".", "\\X", "\\p{M}"])
            parts.append(a + rng.choice(["", "", "", "+", "*", "?", "{2}", "+?"]))
        p = rng.choice(["(?c)", "(?c)", "(?ic)", "(?c)(?-c:x?)", ""]) + "".join(parts)
        if rng.random() < 0.15:
            p = "(?s).*(?<=" + "(?c)" + rng.choice(CANON_SETS) + ")" + rng.choice(["", "a", "x"])
        for _ in range(per):
            yield p, "".join(rng.choice(CANON_SUBJECT) for _ in range(rng.randint(1, 3)))


# ---------------------------------------------------------------------------
# cibackref profile
# ---------------------------------------------------------------------------

CI_CHARS = ["a", "A", "k", "K", "\u212a", "s", "S", "\u017f", "\u00df", "\u1e9e", "\u03c3", "\u03c2", "\u03a3",
            "\u01c5", "\u01c4", "\u01c6", "\U00010400", "\U00010428", "\u0130", "i", "I", "\u0131", "\u00e9",
            "\u00c9", "1", "_", "\u1f80", "\u1f88", "\u1fbc", "\u1fb3", "\u03b9", "\u0345", "\u1fbe"]
CI_ATOMS = ["a", "k", "s", ".", "(?s:.)", "\\w", "[a-z]", "\\p{L}", "\\x{212a}", "\\x{17f}", "\\x{df}", "\\x{3c3}",
            "\\x{1c5}", "\\x{10400}", "\\x{130}", "\\x{131}", "\\x{1f80}", "\\x{345}"]


def cibackref(rng, n_patterns, per):
    for _ in range(n_patterns):
        groups = 0
        parts = []
        for _ in range(rng.randint(1, 4)):
            r = rng.random()
            if r < 0.4 or groups == 0:
                groups += 1
                body = "".join(rng.choice(CI_ATOMS) + rng.choice(["", "", "?", "+", "{1,2}", "*"])
                               for _ in range(rng.randint(1, 2)))
                head = rng.choice(["(", "(", "(?<g%d>" % groups])
                parts.append(head + body + ")" + rng.choice(["", "", "?", "+", "{0,2}", "*"]))
            else:
                k = rng.randint(1, groups)
                ref = "\\%d" % k if rng.random() < 0.8 else "\\k<g%d>" % k
                if "\\k<" in ref and ("(?<g%d>" % k) not in "".join(parts):
                    ref = "\\%d" % k
                flags = rng.choice(["", "(?i:%s)", "(?iu:%s)", "(?-i:%s)"])
                ref = flags % ref if flags else ref
                parts.append(ref + rng.choice(["", "", "?", "+", "{2}", "*", "+?"]))
        p = rng.choice(["(?i)", "(?iu)", "(?iU)", "(?i)", "(?iu)", ""]) + "".join(parts)
        for _ in range(per):
            yield p, "".join(rng.choice(CI_CHARS) for _ in range(rng.randint(0, 6)))


# ---------------------------------------------------------------------------
# emptyloop profile
# ---------------------------------------------------------------------------

EL_ATOMS = ["a", "b", "\\G", "\\A", "^", "$", "\\b", "\\B", "\\z", "\\Z", "(?=a)", "(?!a)", "(?<=a)", "",
            "a?", "b*", "(a?)", "(b*)", "\\1", "\\2", "(?m:^)", "(?m:$)", "\\R?", "\\b{g}"]


def emptyloop(rng, n_patterns, per):
    subs = [""] + ["".join(t) for k in (1, 2, 3, 4) for t in itertools.product(["a", "b", "\n", "\u0301"], repeat=k)]
    for _ in range(n_patterns):
        alts = []
        for _ in range(rng.randint(1, 3)):
            alts.append("".join(rng.choice(EL_ATOMS) for _ in range(rng.randint(0, 2))))
        body = "|".join(alts)
        head = rng.choice(["(?:", "(", "(?:", "(?<g1>"])
        q = rng.choice(["{2}", "{2,3}", "{3}", "*", "+", "{0,2}", "{1,2}", "{2}?", "{2,}", "*?", "{2}+", "?"])
        p = head + body + ")" + q
        if rng.random() < 0.5:
            p += rng.choice(["\\1", "\\1?", "a", "b?", "$", "(?<=\\1)", "(\\1)\\2"])
        if rng.random() < 0.3:
            p = rng.choice(["a?", "(a)", "b*", "(?m)", "(?d)"]) + p
        for s in rng.sample(subs, per):
            yield p, s


# ---------------------------------------------------------------------------
# lookaround profile
# ---------------------------------------------------------------------------

LA_ATOMS = ["\\b", "\\B", "(?=a)", "(?!\\p{L})", "(?<=a)", "(?<!\\b)", "\\p{L}", ".", "a", "[ab]", "\\X",
            "\\b{g}", "(?U:\\b)", "\\x{10428}", "\\x{301}", "\\w", "\\1", "(a)", "(?i:\\x{212a})"]
LA_SUBJECT = ["a", "b", "\U00010428", "\u0301", "\u00e9", " ", "\U0001F600", "k", "\u212a", "_"]


def lookaround(rng, n_patterns, per):
    subs = [""] + ["".join(t) for k in (1, 2, 3) for t in itertools.product(LA_SUBJECT, repeat=k)]
    for _ in range(n_patterns):
        body = "".join(rng.choice(LA_ATOMS) + rng.choice(["", "", "?", "{1,2}", "{0,2}"])
                       for _ in range(rng.randint(1, 3)))
        if rng.random() < 0.03:
            # Windows whose length overflows `int`. A zero-width body repeated
            # a billion times takes Java seconds, so it gets a small count.
            n = rng.choice([1073741824, 2147483647, 1073741823, 65536])
            body = rng.choice(["a{%d}", "(?:ab){%d}", "a{%d}b{%d}"]).replace("%d", str(n))
            if rng.random() < 0.2:
                body = "\\b{65536}" + body
        look = rng.choice(["(?<=", "(?<!", "(?=", "(?!"]) + body + ")"
        if rng.random() < 0.2:
            look = rng.choice(["(?<=", "(?<!"]) + rng.choice(["(?=", "(?!", "(?<=", "(?<!"]) + body + ")" + \
                rng.choice(["a", ".", "\\b", ""]) + ")"
        pre = rng.choice(["(?s).*", "(a)?", ".", "", "\\p{L}*", "(?s).*?"])
        post = rng.choice(["(?s).*", "", ".", "\\1?", "\\p{L}?"])
        p = rng.choice(["", "", "(?U)", "(?i)", "(?iu)"]) + pre + look + post
        for s in rng.sample(subs, per):
            yield p, s


PROFILES = {
    "anchors": anchors,
    "all": all_constructs,
    "unicode": unicode_props,
    "lookbehind": lookbehind,
    "backref": backref,
    "javaprops": javaprops,
    "names": names,
    "grapheme": grapheme,
    "canon": canon,
    "cibackref": cibackref,
    "emptyloop": emptyloop,
    "lookaround": lookaround,
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--patterns", type=int, default=1000)
    ap.add_argument("--subjects", type=int, default=8, help="subjects per pattern")
    ap.add_argument("--profile", choices=sorted(PROFILES), default="all")
    args = ap.parse_args()
    rng = random.Random(args.seed)
    gen = PROFILES[args.profile]
    for p, s in gen(rng, args.patterns, args.subjects):
        print(esc(p) + "\t" + esc(s))


if __name__ == "__main__":
    main()
