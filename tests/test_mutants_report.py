#!/usr/bin/env python3

"""The weekly report is shell inside the workflow. These tests run that very text
against fake results, so a part whose job died is checked on every pull request
instead of on a Saturday."""

import json
import os
import subprocess
from pathlib import Path
from typing import NamedTuple

WORKFLOW = Path(__file__).parent.parent / ".github" / "workflows" / "mutants.yml"
STEP = "List what lived in the open issue"
FAKE_POST = (
    '#!/usr/bin/env bash\ncp "$1" posted.md\nprintf "%s" "$2" > has_findings.txt\n'
)


class Report(NamedTuple):
    posted: str
    has_findings: str
    summary: str


def report_script() -> str:
    lines = WORKFLOW.read_text().split("\n")

    def indent(line: str) -> int:
        return len(line) - len(line.lstrip())

    step = next(i for i, line in enumerate(lines) if line.strip() == f"- name: {STEP}")
    run = next(i for i in range(step + 1, len(lines)) if lines[i].strip() == "run: |")
    body = []
    for line in lines[run + 1 :]:
        if line.strip() and indent(line) <= indent(lines[run]):
            break
        body.append(line[indent(lines[run]) + 2 :])
    return "\n".join(body)


def run_report(
    tmp_path: Path,
    parts: list[int],
    missing: tuple[int, ...] = (),
    lived: dict[int, str] | None = None,
) -> Report:
    for part in parts:
        if part in missing:
            continue
        out = tmp_path / "results" / f"mutants-part-{part}"
        out.mkdir(parents=True)
        (out / "lived.txt").write_text((lived or {}).get(part, ""))
    (tmp_path / "scripts").mkdir()
    post = tmp_path / "scripts" / "mutants-post.sh"
    post.write_text(FAKE_POST)
    post.chmod(0o755)
    summary = tmp_path / "summary.md"
    summary.write_text("")
    # The shell GitHub Actions uses for a run step.
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", report_script()],
        cwd=tmp_path,
        env={
            "PATH": os.environ["PATH"],
            "GH_TOKEN": "fake",
            "GH_REPO": "o/r",
            "RUN": "https://example.test/run",
            "PARTS": json.dumps(parts),
            "JOBS": "failure",
            "GITHUB_STEP_SUMMARY": str(summary),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stderr == ""
    assert result.returncode == 0
    return Report(
        (tmp_path / "posted.md").read_text(),
        (tmp_path / "has_findings.txt").read_text(),
        summary.read_text(),
    )


def lost_line(parts: str) -> str:
    return (
        f"**No results from part {parts}, so its mutants are not listed.** "
        "Rerun the failed jobs of the run."
    )


def test_a_run_with_every_part_is_clean_and_names_nothing_lost(tmp_path):
    report = run_report(tmp_path, [0, 1, 2])
    assert "Every change in these parts was caught by a test." in report.posted
    assert "No results from part" not in report.posted
    assert report.has_findings == "false"


def test_a_lost_part_is_named_and_counts_as_a_finding(tmp_path):
    report = run_report(tmp_path, [0, 1, 2], missing=(1,))
    assert lost_line("1") in report.posted
    assert lost_line("1") in report.summary
    # Nothing was seen of that part, so the report cannot call it all caught.
    assert "Every change in these parts was caught" not in report.posted
    assert report.has_findings == "true"


def test_several_lost_parts_are_named_next_to_the_rows_that_did_arrive(tmp_path):
    report = run_report(
        tmp_path,
        [0, 1, 2, 3],
        missing=(0, 2),
        lived={1: "ytmusicfs/cache.py:3  lived: x\n"},
    )
    assert lost_line("0, 2") in report.posted
    assert "ytmusicfs/cache.py:3  lived: x" in report.posted
    assert report.has_findings == "true"
