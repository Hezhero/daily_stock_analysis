"""End-to-end self-test for the ``DSA_ENV_LEAK_PROBE`` mode (ticket 12).

The probe is a tool for finding cross-test env leaks, so a silent no-op in the
probe itself would hide the very class of bug it exists to catch. These cases
spawn pytest on tiny fixture modules and assert the session report: a clean run
must say so, and a deliberately leaky run must name ``ENV_FILE``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CASES = Path(__file__).parent / "leak_probe_cases"


def _run(case: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DSA_ENV_LEAK_PROBE"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(CASES / case),
            "-q",
            "-s",
            "-p",
            "no:cacheprovider",
        ],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )


def test_probe_reports_a_clean_run():
    result = _run("test_clean.py")
    combined = result.stdout + result.stderr

    assert result.returncode == 0, combined
    assert "[env-leak-probe] clean: no env transitions" in combined


def test_probe_reports_an_env_leak():
    result = _run("test_leaky.py")
    combined = result.stdout + result.stderr

    assert result.returncode == 0, combined
    assert "state transition(s) not restored" in combined
    assert "ENV_FILE" in combined
