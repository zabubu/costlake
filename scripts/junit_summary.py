"""Render JUnit XML reports as a Markdown table for the GitHub job summary.

Usage: python scripts/junit_summary.py reports/*.xml >> "$GITHUB_STEP_SUMMARY"
Failed tests are listed with the first line of their message so that the cause of
a red pipeline is visible without opening the logs.
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET  # noqa: S405 - parses reports produced by our own pytest run
from pathlib import Path


def main(paths: list[str]) -> int:
    print("| report | tests | failures | errors | skipped | time (s) |")
    print("|---|---:|---:|---:|---:|---:|")
    failed: list[str] = []
    for path in paths:
        if not Path(path).exists():
            continue
        root = ET.parse(path).getroot()  # noqa: S314
        suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
        for suite in suites:
            print(
                f"| {Path(path).stem} | {suite.get('tests')} | {suite.get('failures')} "
                f"| {suite.get('errors')} | {suite.get('skipped')} | {float(suite.get('time', 0)):.1f} |"
            )
            for case in suite.iter("testcase"):
                for problem in (*case.findall("failure"), *case.findall("error")):
                    message = (problem.get("message") or "").splitlines()[0][:200]
                    failed.append(f"- `{case.get('classname')}::{case.get('name')}`: {message}")
    if failed:
        print("\n**Failed tests**\n")
        print("\n".join(failed))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
