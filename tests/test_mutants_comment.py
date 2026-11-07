#!/usr/bin/env python3

import os
import subprocess
from pathlib import Path
from typing import NamedTuple

import pytest

SCRIPT = Path(__file__).parent.parent / "scripts" / "mutants-comment.sh"
CALL_END = "\x01"

# Records every call and answers the lookup of the existing comment (the one `--jq`
# call) with FAKE_GH_OLD, like the real one prints the id it found, if any.
FAKE_GH = """\
#!/usr/bin/env bash
{ printf '%s\\0' "$@"; printf '\\1'; } >> "$FAKE_GH_LOG"
case " $* " in *" --jq "*) printf '%s' "${FAKE_GH_OLD:-}" ;; esac
"""


class Run(NamedTuple):
    calls: list[list[str]]
    summary: str
    code: int


@pytest.fixture
def comment(tmp_path):
    """Runs the comment script on a results directory with a fake `gh`."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh = bin_dir / "gh"
    gh.write_text(FAKE_GH)
    gh.chmod(0o755)

    def run(
        lived: str | None, tally: str | None, *, old: str = "", fork: str = "false"
    ):
        results = tmp_path / "results"
        results.mkdir(exist_ok=True)
        if lived is not None:
            (results / "lived.txt").write_text(lived)
        if tally is not None:
            (results / "tally.txt").write_text(tally)
        log = tmp_path / "gh.log"
        log.unlink(missing_ok=True)
        summary = tmp_path / "summary.md"
        summary.unlink(missing_ok=True)
        env = {
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "GH_TOKEN": "fake",
            "GITHUB_REPOSITORY": "owner/repo",
            "PR": "7",
            "RUN": "https://example.test/run/1",
            "FORK": fork,
            "GITHUB_STEP_SUMMARY": str(summary),
            "FAKE_GH_LOG": str(log),
            "FAKE_GH_OLD": old,
        }
        done = subprocess.run(
            ["bash", str(SCRIPT), str(results)],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        entries = log.read_text().split(CALL_END)[:-1] if log.exists() else []
        return Run(
            [entry.split("\0")[:-1] for entry in entries],
            summary.read_text() if summary.exists() else "",
            done.returncode,
        )

    return run


LIVED = "ytmusicfs/cache.py:12  survived: ttl > 0 -> ttl >= 0\n"


class TestMutantsComment:
    def test_comment_creates_one_when_something_lived_and_there_is_none(self, comment):
        run = comment(LIVED, "9 of 10\n")

        assert run.code == 0
        lookup, create = run.calls
        assert lookup[:3] == ["api", "--paginate", "repos/owner/repo/issues/7/comments"]
        assert create[:2] == ["api", "repos/owner/repo/issues/7/comments"]
        body = create[-1].removeprefix("body=")
        assert body.startswith("<!-- mutants -->\n")
        assert "**Mutation testing:** 9 of 10 changes caught by a test." in body
        assert LIVED.strip() in body
        assert "[Full report](https://example.test/run/1)" in body

    def test_comment_edits_the_existing_one_in_place(self, comment):
        run = comment(LIVED, "9 of 10\n", old="4242")

        assert len(run.calls) == 2
        assert run.calls[1][:4] == [
            "api",
            "-X",
            "PATCH",
            "repos/owner/repo/issues/comments/4242",
        ]
        assert "9 of 10" in run.calls[1][-1]

    def test_comment_is_deleted_once_everything_is_caught(self, comment):
        run = comment("", "10 of 10\n", old="4242")

        assert run.code == 0
        assert run.calls[1] == [
            "api",
            "-X",
            "DELETE",
            "repos/owner/repo/issues/comments/4242",
        ]
        assert run.summary == ""

    def test_comment_is_not_created_when_everything_is_caught(self, comment):
        run = comment("", "10 of 10\n")

        assert len(run.calls) == 1
        assert run.summary == ""

    def test_comment_looks_for_its_marker_among_the_bots_comments(self, comment):
        lookup = comment(LIVED, "9 of 10\n").calls[0]

        jq = lookup[lookup.index("--jq") + 1]
        assert 'select(.user.login == "github-actions[bot]"' in jq
        assert 'startswith("<!-- mutants -->")' in jq

    def test_comment_from_a_fork_only_writes_the_run_summary(self, comment):
        run = comment(LIVED, "9 of 10\n", fork="true")

        assert run.code == 0
        assert run.calls == []
        assert "<!-- mutants -->" in run.summary
        assert LIVED.strip() in run.summary

    def test_comment_without_results_counts_zero_and_deletes_the_old_one(self, comment):
        run = comment(None, None, old="4242")

        assert run.code == 0
        assert run.calls[1][:3] == ["api", "-X", "DELETE"]

    def test_long_comment_is_bounded_and_keeps_complete_lines_and_fences(self, comment):
        rows = "".join(
            f'ytmusicfs/a.py:{i}  survived: {"ñ" * 40}\n' for i in range(2000)
        )
        run = comment(rows, "0 of 2000\n")
        body = run.calls[-1][-1].removeprefix("body=")

        assert run.code == 0
        assert len(body.encode()) <= 60000
        assert body.count("```") == 2
        assert "[Full report](https://example.test/run/1)" in body
        assert all(
            line in rows.splitlines()
            for line in body.split("```")[1].strip().splitlines()
        )
