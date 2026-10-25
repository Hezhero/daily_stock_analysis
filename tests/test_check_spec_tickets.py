# -*- coding: utf-8 -*-
"""Contract tests for scripts/check_spec_tickets.py."""

from __future__ import annotations

from pathlib import Path

from scripts.check_spec_tickets import check_tickets, main

VALID_V2 = """# 05: demo

**Blocked by:** None

**Gate:** `v2-portfolio`
**Signal set:** `unchanged`

**Status:** ready-for-agent
"""


def _write(root: Path, name: str, body: str) -> Path:
    path = root / "docs" / "specs" / "demo" / "issues" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def test_valid_ticket_passes(tmp_path: Path) -> None:
    _write(tmp_path, "05-valid.md", VALID_V2)
    assert check_tickets(tmp_path) == {}


def test_missing_gate_is_reported(tmp_path: Path) -> None:
    _write(tmp_path, "01-missing.md", "# demo\n\n**Blocked by:** None\n\n**Signal set:** `none`\n")
    failures = check_tickets(tmp_path)
    assert len(failures) == 1
    assert "missing **Gate:**" in next(iter(failures.values()))


def test_v2_with_changed_signal_set_is_rejected(tmp_path: Path) -> None:
    _write(tmp_path, "02-v2.md", VALID_V2.replace("`unchanged`", "`changed`"))
    failures = check_tickets(tmp_path)
    assert "flip-gate spec §9" in " ".join(next(iter(failures.values())))


def test_none_gate_requires_none_signal_set(tmp_path: Path) -> None:
    _write(tmp_path, "03-none.md", VALID_V2.replace("`v2-portfolio`", "`none`"))
    failures = check_tickets(tmp_path)
    assert "non-experiment ticket" in " ".join(next(iter(failures.values())))


def test_v1_with_changed_signal_set_requires_rationale(tmp_path: Path) -> None:
    body = VALID_V2.replace("`v2-portfolio`", "`v1-signal`").replace("`unchanged`", "`changed`")
    _write(tmp_path, "04-v1-no-rationale.md", body)
    failures = check_tickets(tmp_path)
    assert "**Gate rationale:**" in " ".join(next(iter(failures.values())))

    _write(
        tmp_path,
        "04-v1-rationale.md",
        body.replace("**Status:**", "**Gate rationale:** docs/adr/0004-demo.md\n\n**Status:**"),
    )
    path = tmp_path / "docs/specs/demo/issues/04-v1-no-rationale.md"
    path.unlink()
    assert check_tickets(tmp_path) == {}


def test_main_exit_codes(tmp_path: Path, capsys) -> None:
    _write(tmp_path, "05-valid.md", VALID_V2)
    assert main([str(tmp_path)]) == 0
    assert "OK: 1 ticket" in capsys.readouterr().out

    _write(tmp_path, "06-bad.md", "# demo\n")
    assert main([str(tmp_path)]) == 1
    assert "FAIL" in capsys.readouterr().out
