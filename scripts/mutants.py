"""Mutation testing: mutmut changes a function a little, one change at a time, and runs
the tests that reach it. A change no test notices is a missing test.

    python -m scripts.mutants diff origin/main     # the lines this branch changed
    python -m scripts.mutants part 3 64            # the 4th of 64 equal parts of all lines
    python -m scripts.mutants diff HEAD^1 --out build/mutants

mutmut 3 works on whole functions, not on line ranges, and it doesn't say which line
of the source a change is on. So this script:

1. picks the source lines (the changed ones, or one part of all of them);
2. runs mutmut on the mutants of every function those lines touch;
3. works out each mutant's source line by diffing its function against the unchanged
   copy mutmut writes next to it in mutants/;
4. keeps only the mutants on the picked lines, and lists what lived by file and line.

Only what the coverage gate counts (``[tool.coverage.run]`` in pyproject.toml) is
changed, and mutmut only changes code inside functions and methods. --out also writes
the list to lived.txt and the tally ("3 of 10") to tally.txt there. Exits 0 once it
got to its tally, whatever lived; only a broken run fails.
"""

import argparse
import ast
import difflib
import fnmatch
import json
import os
import shutil
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

ROOT = Path(__file__).resolve().parent.parent
MUTANTS_DIR = "mutants"
CLASS_SEPARATOR = "ǁ"
GENERATED = "__mutmut_"

# mutmut's exit codes per mutant (mutmut/stats.py): what a test run ended with.
CAUGHT_CODES = {1, 3, 37}
SKIPPED_CODES = {34}
STATUS_BY_CODE = {
    0: "survived",
    5: "no tests",
    33: "no tests",
    36: "timeout",
    24: "timeout",
    -24: "timeout",
    152: "timeout",
    255: "timeout",
    -11: "segfault",
    -9: "segfault",
}
DESCRIPTION_WIDTH = 70


@dataclass(frozen=True)
class Scope:
    """The source the coverage gate counts."""

    sources: tuple[str, ...]
    omit: tuple[str, ...]


@dataclass(frozen=True)
class Range:
    """A range of lines of one file, 1-based and inclusive."""

    file: str
    start: int
    end: int


@dataclass(frozen=True)
class Located:
    """Where a mutant changed its function, in the lines of the original source."""

    first: int
    last: int
    old: str
    new: str


@dataclass(frozen=True)
class Mutant:
    file: str
    name: str
    code: int | None
    where: Located


def load_scope(pyproject: Path) -> Scope:
    run = tomllib.loads(pyproject.read_text("utf-8"))["tool"]["coverage"]["run"]
    return Scope(tuple(run["source"]), tuple(run.get("omit", [])))


def in_scope(file: str, scope: Scope) -> bool:
    """Whether `file` is a source file the coverage gate counts."""
    return (
        file.endswith(".py")
        and any(file.startswith(f"{source}/") for source in scope.sources)
        and not any(
            fnmatch.fnmatch(file, pattern)
            or fnmatch.fnmatch(f"/{file}", f"*/{pattern}")
            for pattern in scope.omit
        )
    )


def changed_ranges(diff: str) -> list[Range]:
    """The ranges of lines a unified diff with no context (`-U0`) adds or changes."""
    ranges: list[Range] = []
    file: str | None = None
    for line in diff.split("\n"):
        if line.startswith("+++ "):
            path = line[4:]
            file = None if path == "/dev/null" else path.removeprefix("b/")
        elif file and line.startswith("@@ "):
            plus = next(word for word in line.split() if word.startswith("+"))
            start, _, count = plus[1:].partition(",")
            size = int(count) if count else 1
            if size > 0:
                ranges.append(Range(file, int(start), int(start) + size - 1))
    return ranges


def part_ranges(files: Sequence[tuple[str, int]], k: int, n: int) -> list[Range]:
    """Every line of every file, in order, cut into `n` parts as equal as whole
    lines allow (sizes differ by at most one); the `k`th, 0-based, as ranges."""
    if not 0 <= k < n:
        raise ValueError(f"part {k} isn't one of 0 to {n - 1}")
    total = sum(lines for _, lines in files)
    first = total * k // n
    last = total * (k + 1) // n
    ranges: list[Range] = []
    offset = 0
    for file, lines in files:
        start = max(first, offset)
        end = min(last, offset + lines)
        if start < end:
            ranges.append(Range(file, start - offset + 1, end - offset))
        offset += lines
    return ranges


def line_count(text: str) -> int:
    return text.count("\n") + (0 if text.endswith("\n") or not text else 1)


def source_files(root: Path, scope: Scope) -> list[tuple[str, int]]:
    """Every source file in scope with its number of lines, in a fixed order."""
    found = sorted(
        path.relative_to(root).as_posix()
        for source in scope.sources
        for path in (root / source).rglob("*.py")
    )
    return [
        (file, line_count((root / file).read_text("utf-8")))
        for file in found
        if in_scope(file, scope)
    ]


def _defs(
    tree: ast.Module,
) -> Iterator[tuple[str | None, ast.FunctionDef | ast.AsyncFunctionDef]]:
    """The functions and methods mutmut can change: top-level ones, and those of
    top-level classes. Each comes with the name of its class, if it has one."""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            yield None, node
        elif isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                    yield node.name, child


def mangle(class_name: str | None, name: str) -> str:
    """How mutmut names a function in its mutants (mutmut/mutation/trampoline_templates.py)."""
    if class_name:
        return f"x{CLASS_SEPARATOR}{class_name}{CLASS_SEPARATOR}{name}"
    return f"x_{name}"


def module_name(file: str) -> str:
    return file.removesuffix(".py").replace("/", ".")


def mutant_pattern(file: str, mangled: str) -> str:
    """The pattern `mutmut run` takes for every mutant of one function."""
    return f"{module_name(file)}.{mangled}{GENERATED}*".replace(".__init__.", ".")


def touched_functions(file: str, source: str, ranges: Sequence[Range]) -> list[str]:
    """The mangled names of the functions of `file` that overlap `ranges`."""
    return [
        mangle(class_name, node.name)
        for class_name, node in _defs(ast.parse(source))
        if node.end_lineno is not None
        and any(
            r.file == file and r.start <= node.end_lineno and node.lineno <= r.end
            for r in ranges
        )
    ]


def _lines_of(
    lines: Sequence[str], node: ast.FunctionDef | ast.AsyncFunctionDef
) -> list[str]:
    """The lines of a function, from its `def`, without its own name: the unchanged
    copy and each mutant are named differently, and that is not a change."""
    body = list(lines[node.lineno - 1 : node.end_lineno])
    body[0] = body[0].replace(node.name, "", 1)
    return body


def _shorten(lines: Sequence[str]) -> str:
    text = " ".join(line.strip() for line in lines)
    if len(text) <= DESCRIPTION_WIDTH:
        return text
    return text[: DESCRIPTION_WIDTH - 3] + "..."


def _where(
    start: int, original: Sequence[str], mutated: Sequence[str], size: int
) -> Located:
    """Where `mutated` differs from `original`, a function that starts on line `start`
    of the source and takes `size` lines there."""
    if len(original) != size:
        # The copy doesn't line up with the source: all we know is the function.
        return Located(start, start, "", "")
    changes = [
        op
        for op in difflib.SequenceMatcher(
            None, original, mutated, autojunk=False
        ).get_opcodes()
        if op[0] != "equal"
    ]
    if not changes:
        return Located(start, start, "", "")
    # A change that only adds lines is on the line it was added before (or the last).
    first = min(changes[0][1], len(original) - 1)
    last = max(changes[-1][2] - 1, first)
    return Located(
        start + first,
        start + last,
        _shorten(original[changes[0][1] : changes[-1][2]]),
        _shorten(mutated[changes[0][3] : changes[-1][4]]),
    )


def locate(source: str, mutated: str) -> dict[str, Located]:
    """For each mutant function in `mutated` (the file mutmut wrote), the lines of
    `source` it changed. The unchanged copy mutmut keeps next to each mutant is the
    original function verbatim, so diffing the two shows the change."""
    starts = {mangle(c, node.name): node for c, node in _defs(ast.parse(source))}
    lines = mutated.split("\n")
    nodes = {node.name: node for _, node in _defs(ast.parse(mutated))}
    found: dict[str, Located] = {}
    for name, node in nodes.items():
        base, separator, suffix = name.partition(GENERATED)
        original = nodes.get(f"{base}{GENERATED}orig")
        origin = starts.get(base)
        if not separator or suffix == "orig" or original is None or origin is None:
            continue
        size = (origin.end_lineno or origin.lineno) - origin.lineno + 1
        found[name] = _where(
            origin.lineno, _lines_of(lines, original), _lines_of(lines, node), size
        )
    return found


def collect(root: Path, file: str) -> list[Mutant]:
    """Every mutant of `file` with its result, from the files mutmut left in mutants/."""
    meta = json.loads((root / MUTANTS_DIR / f"{file}.meta").read_text("utf-8"))
    mutated = (root / MUTANTS_DIR / file).read_text("utf-8")
    where = locate((root / file).read_text("utf-8"), mutated)
    return [
        Mutant(file, key, code, where[key.rpartition(".")[2]])
        for key, code in meta["exit_code_by_key"].items()
        if key.rpartition(".")[2] in where
    ]


def select(
    mutants: Sequence[Mutant], ranges: Sequence[Range], *, by_start: bool
) -> list[Mutant]:
    """The mutants on `ranges`: any that changed a line in them, or, with `by_start`,
    those whose first changed line is in them (so a part of all lines owns each once).
    """

    def on(mutant: Mutant, r: Range) -> bool:
        if r.file != mutant.file:
            return False
        first, last = mutant.where.first, mutant.where.last
        if by_start:
            return r.start <= first <= r.end
        return first <= r.end and r.start <= last

    return [m for m in mutants if any(on(m, r) for r in ranges)]


def lived(mutants: Sequence[Mutant]) -> tuple[list[str], int]:
    """What no test caught, "ytmusicfs/a.py:12  survived: x < 3 -> x <= 3", by file
    and line, and how many mutants there were (skipped ones don't count)."""
    counted = [m for m in mutants if m.code not in SKIPPED_CODES]
    left = sorted(
        (m for m in counted if m.code not in CAUGHT_CODES),
        key=lambda m: (m.file, m.where.first, m.name),
    )
    rows = []
    for m in left:
        status = (
            STATUS_BY_CODE.get(m.code, "suspicious")
            if m.code is not None
            else "not checked"
        )
        change = (
            f": {m.where.old} -> {m.where.new}" if m.where.old or m.where.new else ""
        )
        rows.append(f"{m.file}:{m.where.first}  {status}{change} [{m.name}]")
    return rows, len(counted)


def run_mutmut(patterns: Sequence[str]) -> int:
    """Run mutmut on the mutants matching `patterns`, from the repository root."""
    executable = shutil.which("mutmut")
    if executable is None:
        print(
            "mutmut isn't installed: pip install -e '.[dev,mutants]'", file=sys.stderr
        )
        return 127
    return subprocess.run(
        [executable, "run", *patterns], cwd=ROOT, check=False
    ).returncode


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {result.stderr}")
    return result.stdout


def pick_ranges(args: argparse.Namespace, scope: Scope) -> list[Range]:
    if args.mode == "diff":
        since = git("merge-base", args.base, "HEAD").strip()
        diff = git("diff", "-U0", "--no-color", since, "HEAD")
        return [r for r in changed_ranges(diff) if in_scope(r.file, scope)]
    return part_ranges(source_files(ROOT, scope), args.k, args.n)


def mutate(ranges: Sequence[Range], *, by_start: bool) -> tuple[list[Mutant], bool]:
    """Run mutmut on the functions `ranges` touch; the mutants on `ranges`, and
    whether the run went through."""
    files = sorted({r.file for r in ranges})
    patterns = [
        mutant_pattern(file, mangled)
        for file in files
        for mangled in touched_functions(file, (ROOT / file).read_text("utf-8"), ranges)
    ]
    if not patterns:
        print("No function on these lines to mutate.")
        return [], True
    shutil.rmtree(ROOT / MUTANTS_DIR, ignore_errors=True)
    exit_code = run_mutmut(patterns)
    metas = [ROOT / MUTANTS_DIR / f"{file}.meta" for file in files]
    if not all(meta.exists() for meta in metas):
        print(
            f"mutmut stopped (exit {exit_code}) before it wrote its mutants.",
            file=sys.stderr,
        )
        return [], False
    mutants = select(
        [m for file in files for m in collect(ROOT, file)], ranges, by_start=by_start
    )
    # mutmut stops with an error when no function it was given has a mutant: that's
    # nothing to test. Any other stop leaves some of the mutants unchecked.
    unchecked = [m for m in mutants if m.code is None]
    if unchecked:
        print(
            f"mutmut stopped (exit {exit_code}) with {len(unchecked)} mutants unchecked.",
            file=sys.stderr,
        )
    return mutants, not unchecked


def parse(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.mutants", description=__doc__
    )
    # On each mode, after its own arguments, the way the workflow spells it.
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--out", help="also write lived.txt and tally.txt here")
    modes = parser.add_subparsers(dest="mode", required=True)
    diff = modes.add_parser(
        "diff", parents=[shared], help="the lines changed since BASE"
    )
    diff.add_argument("base")
    part = modes.add_parser(
        "part", parents=[shared], help="part K of N equal parts of all source lines"
    )
    part.add_argument("k", type=int)
    part.add_argument("n", type=int)
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse(argv)
    os.chdir(ROOT)
    ranges = pick_ranges(args, load_scope(ROOT / "pyproject.toml"))
    mutants, ok = mutate(ranges, by_start=args.mode == "part")
    if not ok:
        return 1
    rows, total = lived(mutants)
    for row in rows:
        print(row)
    tally = f"{total - len(rows)} of {total}"
    print(f"{tally} changes caught by a test.")
    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "lived.txt").write_text("".join(f"{row}\n" for row in rows), "utf-8")
        (out / "tally.txt").write_text(f"{tally}\n", "utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
