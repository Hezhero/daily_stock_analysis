#!/usr/bin/env python3
"""Compare the native-Windows pytest failure set with its documented baseline.

Ticket: ``docs/specs/backtest_度量层重建/issues/10-native-windows-baseline-whitelist.md``

Exit codes: ``0`` baseline matches, ``1`` findings, ``2`` usage/environment error.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = REPO_ROOT / "docs" / "agents" / "windows-known-failures.md"
DEFAULT_PYTEST_ARGS = ("-m", "not network", "--timeout=120", "-o", "timeout_method=thread")
ALLOWED_CATEGORIES = (
    "product-excluded",
    "stdlib-missing",
    "posix-mode-bits",
    "known-unattributed",
)
TODO = "TODO"

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_USAGE = 2

BASELINE_HEADER = """\
# native Windows 已知失败白名单

`python -m pytest -m "not network"` 在 native Windows 上不构成全量基线（见
`docs/agents/validation-matrix.md` 的 Platform boundary）。本文件登记**已经归因**的既有失败，
让"新增失败""白名单过期""未填理由"三类问题各自显性，而不是混在同一堆红字里。

- 生成/更新：`python scripts/check_windows_test_baseline.py --update`
- 校验：`python scripts/check_windows_test_baseline.py`
- `类别` 取 `product-excluded` / `stdlib-missing` / `posix-mode-bits`（定义见 validation-matrix），
  或 `known-unattributed`：既有失败、根因已记录但**不属于**任何平台契约。后者只用于把已知失败固定住，
  让新增失败显性；它不是"平台允许的差异"，不允许用它掩盖平台契约问题。
- `理由` 写作 `TODO` 表示尚未归因；checker 会因此失败，**不要**把未分类失败当成已归因。
- 本文件只登记失败，不改变测试结果；真正的平台契约排除仍应写成 `skipif`。
- `test` 用 JUnit 的 `classname::name`，与 `pytest -rf` 打印的路径形式不逐字相同，新增条目请走 `--update`。
- **登记前提是该用例在目标平台上确定性失败**。偶发失败（同机两次跑结果不同）不是已知失败，
  必须修掉或显式隔离，不能靠白名单吸收；否则 `NEW` / `STALE` 会随机抖动。

| test | 类别 | 理由 |
| --- | --- | --- |
"""


@dataclass(frozen=True)
class Entry:
    test: str
    category: str
    reason: str


@dataclass
class Outcome:
    failures: set[str] = field(default_factory=set)
    collection_errors: set[str] = field(default_factory=set)


@dataclass
class Report:
    new: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    unresolved: list[str] = field(default_factory=list)
    bad_category: list[str] = field(default_factory=list)
    collection_errors: list[str] = field(default_factory=list)
    stale_soft: list[str] = field(default_factory=list)
    unrepresented: int = 0

    @property
    def ok(self) -> bool:
        return not (
            self.new
            or self.stale
            or self.unresolved
            or self.bad_category
            or self.collection_errors
        )

    def exit_code(self) -> int:
        return EXIT_OK if self.ok else EXIT_FINDINGS


def parse_baseline(text: str) -> list[Entry]:
    """Read the markdown table rows; everything else is prose and ignored."""
    entries: list[Entry] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) != 3:
            continue
        test, category, reason = (cell.strip("`").strip() for cell in cells)
        if test in {"test", ""} or set(test) <= {"-", " "}:
            continue
        entries.append(Entry(test=test, category=category, reason=reason))
    return entries


def render_baseline(entries: Iterable[Entry]) -> str:
    rows = [
        f"| `{entry.test}` | {entry.category} | {entry.reason} |"
        for entry in sorted(entries, key=lambda item: item.test)
    ]
    return BASELINE_HEADER + "\n".join(rows) + ("\n" if rows else "")


def read_junit(path: Path) -> Outcome:
    outcome = Outcome()
    for case in ET.parse(path).getroot().iter("testcase"):
        if case.find("failure") is None and case.find("error") is None:
            continue
        classname = case.get("classname", "")
        name = case.get("name", "")
        test_id = f"{classname}::{name}"
        # pytest reports a collection failure as a testcase named after the module.
        if name.endswith(".py") or name == classname:
            outcome.collection_errors.add(test_id)
        else:
            outcome.failures.add(test_id)
    return outcome


def compare(
    outcome: Outcome,
    entries: Sequence[Entry],
    reported_failures: Optional[int] = None,
) -> Report:
    baseline = {entry.test: entry for entry in entries}
    actual = set(outcome.failures)
    report = Report()
    report.new = sorted(actual - set(baseline))
    disappeared = set(baseline) - actual
    # ``known-unattributed`` entries are tracked failures, not platform contracts:
    # one passing again is good news, so it is reported but must not fail the run.
    report.stale = sorted(
        test
        for test in disappeared
        if baseline[test].category != "known-unattributed"
    )
    report.stale_soft = sorted(
        test
        for test in disappeared
        if baseline[test].category == "known-unattributed"
    )
    report.collection_errors = sorted(outcome.collection_errors)
    for test, entry in baseline.items():
        if entry.reason.strip().upper() == TODO or entry.category.strip().upper() == TODO:
            report.unresolved.append(test)
        elif entry.category not in ALLOWED_CATEGORIES:
            report.bad_category.append(f"{test} ({entry.category})")
    if reported_failures is not None:
        # pytest counts subtest failures that its JUnit report cannot express, so a
        # mismatch means some failures have no nodeid and cannot be baselined.
        known = len(outcome.failures) + len(outcome.collection_errors)
        report.unrepresented = max(0, int(reported_failures) - known)
    report.unresolved.sort()
    report.bad_category.sort()
    return report


def render_report(report: Report, known: int, actual: int) -> str:
    lines = [f"windows-baseline: {known} registered / {actual} failing"]

    def section(title: str, items: Sequence[str], hint: str) -> None:
        if not items:
            return
        lines.append(f"\n{title} ({len(items)}) - {hint}")
        lines.extend(f"  - {item}" for item in items)

    section("NEW", report.new, "failing but unregistered: fix it, or --update and write a reason")
    section("STALE", report.stale, "registered but no longer failing: delete the row")
    section(
        "STALE (tracked, not gating)",
        report.stale_soft,
        "known-unattributed entry passed this run: fine, prune it when it stays green",
    )
    section("UNRESOLVED", report.unresolved, "reason or category is still TODO: attribute it first")
    section("BAD_CATEGORY", report.bad_category, "category must be one of " + ", ".join(ALLOWED_CATEGORIES))
    section("COLLECTION_ERROR", report.collection_errors, "collection errors must be zero; never register them")
    if report.unrepresented:
        lines.append(
            f"\nUNREPRESENTED ({report.unrepresented}) - informational: failures pytest reported "
            "that the JUnit report cannot see (subtest-level), so they cannot be baselined"
        )
    if report.ok:
        lines.append("\nOK: failure set matches the baseline.")
    return "\n".join(lines)


def _run_pytest(junit_path: Path) -> tuple[int, str]:
    command = [
        sys.executable,
        "-m",
        "pytest",
        *DEFAULT_PYTEST_ARGS,
        "-q",
        "--tb=no",
        "--continue-on-collection-errors",
        f"--junitxml={junit_path}",
    ]
    print("==> windows-baseline: " + " ".join(command), flush=True)
    process = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
    )
    chunks: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="")
        chunks.append(line)
    process.wait()
    return process.returncode, "".join(chunks)


def _reported_failures(output: str) -> Optional[int]:
    match = re.search(r"(\d+) failed", output or "")
    return int(match.group(1)) if match else None


def _write_baseline(path: Path, outcome: Outcome) -> list[Entry]:
    previous = {
        entry.test: entry for entry in parse_baseline(path.read_text(encoding="utf-8"))
    } if path.exists() else {}
    entries = []
    for test in sorted(outcome.failures):
        old = previous.get(test)
        entries.append(
            Entry(
                test=test,
                category=old.category if old else TODO,
                reason=old.reason if old else TODO,
            )
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_baseline(entries), encoding="utf-8")
    return entries


def _is_native_windows() -> bool:
    """Indirection so tests can exercise the POSIX guard without patching ``os.name``."""
    return os.name == "nt"


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default=str(DEFAULT_BASELINE))
    parser.add_argument("--update", action="store_true", help="rewrite the baseline from this run")
    parser.add_argument("--junit", help="reuse this junit xml instead of running pytest")
    args = parser.parse_args(argv)

    baseline_path = Path(args.baseline)
    entries = (
        parse_baseline(baseline_path.read_text(encoding="utf-8"))
        if baseline_path.exists()
        else []
    )
    if not _is_native_windows() and entries:
        print(
            f"ERROR: {baseline_path} is a native-Windows baseline and must be empty on this "
            f"platform; found {len(entries)} entries. POSIX is expected to be fully green.",
            file=sys.stderr,
        )
        return EXIT_USAGE

    reuse = bool(args.junit) and Path(args.junit).exists()
    reported: Optional[int] = None
    if reuse:
        junit_path = Path(args.junit)
        print(f"==> windows-baseline: reusing {junit_path}")
    elif args.junit:
        junit_path = Path(args.junit)
        _, output = _run_pytest(junit_path)
        reported = _reported_failures(output)
    else:
        handle, name = tempfile.mkstemp(suffix=".xml")
        os.close(handle)
        junit_path = Path(name)
        _, output = _run_pytest(junit_path)
        reported = _reported_failures(output)

    if not junit_path.exists():
        print(
            "ERROR: pytest produced no junit report (is pytest installed? see "
            "docs/agents/validation-matrix.md).",
            file=sys.stderr,
        )
        return EXIT_USAGE

    outcome = read_junit(junit_path)

    if args.update:
        if outcome.collection_errors:
            print(render_report(compare(outcome, entries, reported), len(entries), len(outcome.failures)))
            print(
                "ERROR: refusing to update the baseline while collection errors exist.",
                file=sys.stderr,
            )
            return EXIT_FINDINGS
        written = _write_baseline(baseline_path, outcome)
        report = compare(outcome, written, reported)
        print(render_report(report, len(written), len(outcome.failures)))
        return report.exit_code()

    report = compare(outcome, entries, reported)
    print(render_report(report, len(entries), len(outcome.failures)))
    return report.exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
