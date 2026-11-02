#!/usr/bin/env python3

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

import pytest

from scripts import mutants
from scripts.mutants import Located, Mutant, Range, Scope

SCOPE = Scope(("ytmusicfs",), ("ytmusicfs/protocols.py",))

SOURCE = """\
def clamp(value, low):
    if value < low:
        return low
    return value


class Box:
    def area(self):
        return self.w * self.h
"""

# What mutmut writes for SOURCE: per function a trampoline, an unchanged copy
# (`__mutmut_orig`) and one copy per change (`__mutmut_N`), then the next one.
# Written by hand after mutmut's code, because the tests don't run mutmut.
MUTATED = """\
from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict


mutants_x_clamp__mutmut: MutantDict = {}  # type: ignore
@_mutmut_mutated(mutants_x_clamp__mutmut)
def clamp(value, low):
    if value < low:
        return low
    return value

def x_clamp__mutmut_orig(value, low):
    if value < low:
        return low
    return value

def x_clamp__mutmut_1(value, low):
    if value <= low:
        return low
    return value

def x_clamp__mutmut_2(value, low):
    if value < low:
        return None
    return value

def x_clamp__mutmut_3(value, low):
    if value < low:
        pass
    return value

def x_clamp__mutmut_4(value, low):
    if value < low:
        return low
    return value
    return value

def x_clamp__mutmut_5(value, low):
    if value < low:
        return low
    return value  # or

mutants_x_clamp__mutmut['_mutmut_orig'] = x_clamp__mutmut_orig
mutants_x_clamp__mutmut['x_clamp__mutmut_1'] = x_clamp__mutmut_1

mutants_xǁBoxǁarea__mutmut: MutantDict = {}  # type: ignore

class Box:
    @_mutmut_mutated(mutants_xǁBoxǁarea__mutmut)
    def area(self):
        return self.w * self.h

    def xǁBoxǁarea__mutmut_orig(self):
        return self.w * self.h

    def xǁBoxǁarea__mutmut_1(self):
        return self.w / self.h

mutants_xǁBoxǁarea__mutmut['_mutmut_orig'] = Box.xǁBoxǁarea__mutmut_orig
"""


def mutant(
    code: int | None,
    first: int = 1,
    last: int | None = None,
    file: str = "ytmusicfs/a.py",
) -> Mutant:
    where = Located(first, first if last is None else last, "old", "new")
    return Mutant(file, f"ytmusicfs.a.x_f__mutmut_{first}", code, where)


class TestScope:
    def test_load_scope_reads_what_the_coverage_gate_counts(self):
        scope = mutants.load_scope(Path(__file__).parent.parent / "pyproject.toml")

        assert scope.sources == ("ytmusicfs",)
        assert "ytmusicfs/protocols.py" in scope.omit

    @pytest.mark.parametrize(
        ("file", "expected"),
        [
            ("ytmusicfs/cache.py", True),
            ("ytmusicfs/__init__.py", True),
            ("ytmusicfs/protocols.py", False),
            ("ytmusicfs/data.json", False),
            ("tests/test_cache.py", False),
            ("scripts/mutants.py", False),
            ("other/ytmusicfs/cache.py", False),
        ],
    )
    def test_in_scope_keeps_only_counted_python_source(self, file, expected):
        assert mutants.in_scope(file, SCOPE) is expected

    def test_in_scope_matches_omit_patterns_with_wildcards(self):
        scope = Scope(("src",), ("*/generated/*",))

        assert not mutants.in_scope("src/generated/api.py", scope)
        assert mutants.in_scope("src/api.py", scope)


class TestChangedRanges:
    DIFF = """\
diff --git a/ytmusicfs/a.py b/ytmusicfs/a.py
--- a/ytmusicfs/a.py
+++ b/ytmusicfs/a.py
@@ -3 +3,2 @@ def f():
@@ -10,2 +11 @@
@@ -20,3 +20,0 @@
@@ -30,0 +28,4 @@
diff --git a/ytmusicfs/gone.py b/ytmusicfs/gone.py
--- a/ytmusicfs/gone.py
+++ /dev/null
@@ -1,5 +0,0 @@
diff --git a/ytmusicfs/new.py b/ytmusicfs/new.py
--- /dev/null
+++ b/ytmusicfs/new.py
@@ -0,0 +1,7 @@
"""

    def test_changed_ranges_lists_added_and_changed_lines_per_file(self):
        assert mutants.changed_ranges(self.DIFF) == [
            Range("ytmusicfs/a.py", 3, 4),
            Range("ytmusicfs/a.py", 11, 11),
            Range("ytmusicfs/a.py", 28, 31),
            Range("ytmusicfs/new.py", 1, 7),
        ]

    def test_changed_ranges_of_an_empty_diff_is_empty(self):
        assert mutants.changed_ranges("") == []


class TestPartRanges:
    @pytest.mark.parametrize(
        "files",
        [
            [("a.py", 10)],
            [("a.py", 7), ("b.py", 0), ("c.py", 1), ("d.py", 120)],
            [("a.py", 3), ("b.py", 3)],
            [("a.py", 1000), ("b.py", 1), ("c.py", 63)],
        ],
    )
    @pytest.mark.parametrize("n", [1, 4, 7, 64, 200])
    def test_part_ranges_cover_every_line_once_in_parts_of_equal_size(self, files, n):
        seen: list[tuple[str, int]] = []
        sizes = []
        for k in range(n):
            lines = [
                (r.file, line)
                for r in mutants.part_ranges(files, k, n)
                for line in range(r.start, r.end + 1)
            ]
            sizes.append(len(lines))
            seen.extend(lines)

        assert sorted(seen) == sorted(
            (file, line) for file, count in files for line in range(1, count + 1)
        )
        assert max(sizes) - min(sizes) <= 1

    def test_part_ranges_cut_in_order_across_files(self):
        files = [("a.py", 6), ("b.py", 6)]

        assert mutants.part_ranges(files, 0, 4) == [Range("a.py", 1, 3)]
        assert mutants.part_ranges(files, 1, 4) == [Range("a.py", 4, 6)]
        assert mutants.part_ranges(files, 2, 4) == [Range("b.py", 1, 3)]
        assert mutants.part_ranges([("a.py", 5), ("b.py", 5)], 1, 2) == [
            Range("b.py", 1, 5)
        ]

    @pytest.mark.parametrize(("k", "n"), [(-1, 4), (4, 4), (0, 0)])
    def test_part_ranges_reject_a_part_out_of_range(self, k, n):
        with pytest.raises(ValueError, match="isn't one of"):
            mutants.part_ranges([("a.py", 10)], k, n)

    def test_source_files_counts_lines_of_sources_in_scope_only(self, tmp_path):
        package = tmp_path / "ytmusicfs"
        package.mkdir()
        (package / "b.py").write_text("x = 1\ny = 2\n")
        (package / "a.py").write_text("x = 1")
        (package / "protocols.py").write_text("x = 1\n")
        (package / "notes.txt").write_text("x\n")

        assert mutants.source_files(tmp_path, SCOPE) == [
            ("ytmusicfs/a.py", 1),
            ("ytmusicfs/b.py", 2),
        ]


class TestFunctions:
    def test_mangle_names_functions_and_methods_as_mutmut_does(self):
        assert mutants.mangle(None, "get") == "x_get"
        assert mutants.mangle("Cache", "get") == "xǁCacheǁget"

    @pytest.mark.parametrize(
        ("file", "mangled", "expected"),
        [
            ("ytmusicfs/cache.py", "x_get", "ytmusicfs.cache.x_get__mutmut_*"),
            (
                "ytmusicfs/cache.py",
                "xǁCacheǁget",
                "ytmusicfs.cache.xǁCacheǁget__mutmut_*",
            ),
            ("ytmusicfs/__init__.py", "x_main", "ytmusicfs.x_main__mutmut_*"),
        ],
    )
    def test_mutant_pattern_matches_every_mutant_of_one_function(
        self, file, mangled, expected
    ):
        assert mutants.mutant_pattern(file, mangled) == expected

    def test_touched_functions_are_those_overlapping_the_ranges(self):
        def touched(*ranges):
            return mutants.touched_functions("ytmusicfs/a.py", SOURCE, ranges)

        assert touched(Range("ytmusicfs/a.py", 1, 1)) == ["x_clamp"]
        assert touched(Range("ytmusicfs/a.py", 4, 4)) == ["x_clamp"]
        assert touched(Range("ytmusicfs/a.py", 5, 7)) == []
        assert touched(Range("ytmusicfs/a.py", 9, 9)) == ["xǁBoxǁarea"]
        assert touched(Range("ytmusicfs/a.py", 1, 20)) == ["x_clamp", "xǁBoxǁarea"]
        assert touched(Range("ytmusicfs/other.py", 1, 20)) == []

    def test_touched_functions_ignore_nested_and_module_level_code(self):
        source = "X = 1\n\n\ndef outer():\n    def inner():\n        return 1\n    return inner\n"

        assert mutants.touched_functions("a.py", source, [Range("a.py", 1, 1)]) == []
        assert mutants.touched_functions("a.py", source, [Range("a.py", 6, 6)]) == [
            "x_outer"
        ]


class TestLocate:
    def test_locate_finds_the_changed_line_of_each_mutant(self):
        found = mutants.locate(SOURCE, MUTATED)

        assert found["x_clamp__mutmut_1"] == Located(
            2, 2, "if value < low:", "if value <= low:"
        )
        assert found["x_clamp__mutmut_2"] == Located(3, 3, "return low", "return None")
        assert found["xǁBoxǁarea__mutmut_1"] == Located(
            9, 9, "return self.w * self.h", "return self.w / self.h"
        )

    def test_locate_finds_removed_and_added_lines(self):
        found = mutants.locate(SOURCE, MUTATED)

        assert found["x_clamp__mutmut_3"] == Located(3, 3, "return low", "pass")
        assert found["x_clamp__mutmut_4"] == Located(4, 4, "", "return value")
        assert found["x_clamp__mutmut_5"] == Located(
            4, 4, "return value", "return value  # or"
        )

    def test_locate_skips_the_trampoline_and_the_unchanged_copy(self):
        assert set(mutants.locate(SOURCE, MUTATED)) == {
            "x_clamp__mutmut_1",
            "x_clamp__mutmut_2",
            "x_clamp__mutmut_3",
            "x_clamp__mutmut_4",
            "x_clamp__mutmut_5",
            "xǁBoxǁarea__mutmut_1",
        }

    def test_locate_falls_back_to_the_function_when_the_source_moved_on(self):
        newer = SOURCE.replace(
            "    return value\n", "    value += 1\n    return value\n", 1
        )

        found = mutants.locate(newer, MUTATED)

        assert found["x_clamp__mutmut_1"] == Located(1, 1, "", "")

    def test_locate_long_changes_are_shortened(self):
        long = "x = " + "1 + " * 40 + "1"
        source = f"def f():\n    {long}\n"
        mutated = (
            f"def x_f__mutmut_orig():\n    {long}\n"
            f"def x_f__mutmut_1():\n    {long.replace('+', '-')}\n"
        )

        where = mutants.locate(source, mutated)["x_f__mutmut_1"]

        assert len(where.old) == len(where.new) == 70
        assert where.old.endswith("...")


class TestSelect:
    def test_select_keeps_mutants_that_changed_a_line_in_the_ranges(self):
        spanning = mutant(0, 10, 14)
        found = mutants.select(
            [mutant(0, 5), mutant(0, 8), spanning, mutant(0, 20)],
            [Range("ytmusicfs/a.py", 8, 12)],
            by_start=False,
        )

        assert found == [mutant(0, 8), spanning]

    def test_select_by_start_keeps_mutants_starting_in_the_ranges(self):
        found = mutants.select(
            [mutant(0, 5, 9), mutant(0, 10, 14), mutant(0, 20)],
            [Range("ytmusicfs/a.py", 8, 12)],
            by_start=True,
        )

        assert found == [mutant(0, 10, 14)]

    @pytest.mark.parametrize("by_start", [False, True])
    def test_select_ignores_other_files(self, by_start):
        found = mutants.select(
            [mutant(0, 9, file="ytmusicfs/b.py")],
            [Range("ytmusicfs/a.py", 1, 100)],
            by_start=by_start,
        )

        assert found == []


class TestLived:
    def test_lived_lists_what_no_test_caught_by_file_and_line(self):
        rows, total = mutants.lived(
            [
                mutant(1, 3),
                mutant(3, 4),
                mutant(0, 30),
                mutant(33, 9),
                mutant(36, 12),
                mutant(99, 40),
                mutant(None, 41),
                mutant(0, 2, file="ytmusicfs/0.py"),
                mutant(-11, 7),
            ]
        )

        assert rows == [
            "ytmusicfs/0.py:2  survived: old -> new",
            "ytmusicfs/a.py:7  segfault: old -> new",
            "ytmusicfs/a.py:9  no tests: old -> new",
            "ytmusicfs/a.py:12  timeout: old -> new",
            "ytmusicfs/a.py:30  survived: old -> new",
            "ytmusicfs/a.py:40  suspicious: old -> new",
            "ytmusicfs/a.py:41  not checked: old -> new",
        ]
        assert total == 9

    def test_lived_does_not_count_skipped_mutants(self):
        assert mutants.lived([mutant(34, 1), mutant(1, 2)]) == ([], 1)

    def test_lived_leaves_out_the_change_when_unknown(self):
        unknown = Mutant("ytmusicfs/a.py", "m", 0, Located(5, 5, "", ""))

        assert mutants.lived([unknown]) == (["ytmusicfs/a.py:5  survived"], 1)

    def test_lived_of_nothing_is_empty(self):
        assert mutants.lived([]) == ([], 0)


class TestCollect:
    def test_collect_reads_results_and_places_each_mutant(self, tmp_path):
        write_project(tmp_path, {"ytmusicfs.a.x_clamp__mutmut_1": 0})

        found = mutants.collect(tmp_path, "ytmusicfs/a.py")

        assert found == [
            Mutant(
                "ytmusicfs/a.py",
                "ytmusicfs.a.x_clamp__mutmut_1",
                0,
                Located(2, 2, "if value < low:", "if value <= low:"),
            )
        ]


def write_project(root: Path, codes: dict[str, int | None]) -> None:
    """A project with SOURCE as ytmusicfs/a.py and mutmut's files for it."""
    package = root / "ytmusicfs"
    (root / "mutants" / "ytmusicfs").mkdir(parents=True)
    package.mkdir(exist_ok=True)
    (package / "a.py").write_text(SOURCE)
    (root / "mutants/ytmusicfs/a.py").write_text(MUTATED)
    (root / "mutants/ytmusicfs/a.py.meta").write_text(
        json.dumps({"exit_code_by_key": codes})
    )


FAKE_MUTMUT = """\
#!{python}
import json, os, shutil, sys
from pathlib import Path

with open(os.environ["FAKE_MUTMUT_LOG"], "a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
fixture = Path(os.environ["FAKE_MUTMUT_FIXTURE"])
if fixture.exists():
    shutil.copytree(fixture, "mutants", dirs_exist_ok=True)
sys.exit(int(os.environ.get("FAKE_MUTMUT_EXIT", "0")))
"""


class Project(NamedTuple):
    root: Path
    fixture: Path
    log: Path


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A git repository with SOURCE committed as ytmusicfs/a.py, and a fake `mutmut`
    that leaves the files in `fixture` in mutants/ the way a run would."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "pyproject.toml").write_text(
        '[tool.coverage.run]\nsource = ["ytmusicfs"]\nomit = ["ytmusicfs/protocols.py"]\n'
    )
    (root / "ytmusicfs").mkdir()
    (root / "ytmusicfs/a.py").write_text(SOURCE)
    for command in (
        ["init", "-q"],
        ["add", "."],
        ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base"],
    ):
        subprocess.run(["git", *command], cwd=root, check=True)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "mutmut"
    fake.write_text(FAKE_MUTMUT.format(python=sys.executable))
    fake.chmod(0o755)
    fixture = tmp_path / "fixture"
    write_project(fixture, {})
    log = tmp_path / "mutmut.log"
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_MUTMUT_LOG", str(log))
    monkeypatch.setenv("FAKE_MUTMUT_FIXTURE", str(fixture / "mutants"))
    monkeypatch.setattr(mutants, "ROOT", root)
    monkeypatch.chdir(tmp_path)
    return Project(root, fixture, log)


def set_results(fixture: Path, codes: dict[str, int | None]) -> None:
    (fixture / "mutants/ytmusicfs/a.py.meta").write_text(
        json.dumps({"exit_code_by_key": codes})
    )


def change_line(root: Path, old: str, new: str) -> None:
    path = root / "ytmusicfs/a.py"
    path.write_text(path.read_text().replace(old, new))
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "change"],
        cwd=root,
        check=True,
    )


class TestMain:
    def test_main_diff_runs_the_touched_functions_and_lists_what_lived(
        self, project, tmp_path, capsys
    ):
        root = project.root
        fixture = project.fixture
        log = project.log
        change_line(root, "return low", "return low + 0")
        set_results(
            fixture,
            {
                "ytmusicfs.a.x_clamp__mutmut_1": 1,
                "ytmusicfs.a.x_clamp__mutmut_2": 0,
                "ytmusicfs.a.x_clamp__mutmut_5": 0,
                "ytmusicfs.a.xǁBoxǁarea__mutmut_1": None,
            },
        )
        out = tmp_path / "out"

        code = mutants.main(["--out", str(out), "diff", "HEAD^"])

        assert code == 0
        assert json.loads(log.read_text()) == ["run", "ytmusicfs.a.x_clamp__mutmut_*"]
        assert (out / "lived.txt").read_text() == (
            "ytmusicfs/a.py:3  survived: return low -> return None\n"
        )
        assert (out / "tally.txt").read_text() == "0 of 1\n"
        assert "0 of 1 changes caught by a test." in capsys.readouterr().out

    def test_main_part_owns_each_mutant_by_its_first_line(self, project, tmp_path):
        fixture = project.fixture
        set_results(
            fixture,
            {
                "ytmusicfs.a.x_clamp__mutmut_1": 1,
                "ytmusicfs.a.x_clamp__mutmut_2": 0,
                "ytmusicfs.a.xǁBoxǁarea__mutmut_1": 1,
            },
        )
        out = tmp_path / "out"

        # The file has 9 lines: part 0 of 3 is lines 1 to 3.
        code = mutants.main(["--out", str(out), "part", "0", "3"])

        assert code == 0
        assert (out / "lived.txt").read_text() == (
            "ytmusicfs/a.py:3  survived: return low -> return None\n"
        )
        assert (out / "tally.txt").read_text() == "1 of 2\n"

    def test_main_with_everything_caught_writes_empty_results(self, project, tmp_path):
        fixture = project.fixture
        set_results(
            fixture,
            {"ytmusicfs.a.x_clamp__mutmut_1": 1, "ytmusicfs.a.x_clamp__mutmut_2": 3},
        )
        out = tmp_path / "out"

        assert mutants.main(["--out", str(out), "part", "0", "3"]) == 0
        assert (out / "lived.txt").read_text() == ""
        assert (out / "tally.txt").read_text() == "2 of 2\n"

    def test_main_without_source_lines_does_not_run_mutmut(
        self, project, tmp_path, capsys
    ):
        root = project.root
        log = project.log
        (root / "README.md").write_text("hi\n")
        (root / "ytmusicfs/protocols.py").write_text("X = 1\n")
        change_line(root, "\n\n\nclass Box", "\n\n\n\nclass Box")
        out = tmp_path / "out"

        code = mutants.main(["--out", str(out), "diff", "HEAD^"])

        assert code == 0
        assert not log.exists()
        assert (out / "tally.txt").read_text() == "0 of 0\n"
        assert "No function on these lines to mutate." in capsys.readouterr().out

    def test_main_treats_a_stop_with_nothing_to_test_as_all_caught(
        self, project, monkeypatch
    ):
        fixture = project.fixture
        # mutmut stops with an error when no function it was given has a mutant;
        # the mutants of the other functions are not run and not wanted.
        set_results(fixture, {"ytmusicfs.a.xǁBoxǁarea__mutmut_1": None})
        monkeypatch.setenv("FAKE_MUTMUT_EXIT", "1")

        assert mutants.main(["part", "0", "9"]) == 0

    def test_main_fails_when_mutmut_stops_before_it_wrote_mutants(
        self, project, monkeypatch, capsys
    ):
        fixture = project.fixture
        monkeypatch.setenv("FAKE_MUTMUT_FIXTURE", str(fixture / "nothing"))
        monkeypatch.setenv("FAKE_MUTMUT_EXIT", "2")

        code = mutants.main(["part", "0", "3"])

        assert code == 1
        assert "before it wrote its mutants" in capsys.readouterr().err

    def test_main_fails_when_mutants_on_the_lines_were_not_checked(
        self, project, monkeypatch, capsys
    ):
        fixture = project.fixture
        set_results(fixture, {"ytmusicfs.a.x_clamp__mutmut_2": None})
        monkeypatch.setenv("FAKE_MUTMUT_EXIT", "1")

        code = mutants.main(["part", "0", "3"])

        assert code == 1
        assert "1 mutants unchecked" in capsys.readouterr().err

    def test_main_fails_when_mutmut_is_missing(self, project, monkeypatch, capsys):
        monkeypatch.setenv("PATH", "/nonexistent")

        code = mutants.main(["part", "0", "3"])

        assert code == 1
        assert "mutmut isn't installed" in capsys.readouterr().err

    def test_main_rejects_a_part_out_of_range(self, project):
        with pytest.raises(ValueError, match="isn't one of"):
            mutants.main(["part", "3", "3"])
