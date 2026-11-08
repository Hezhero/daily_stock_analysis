# -*- coding: utf-8 -*-
"""Contract tests for scripts/check_spec_tickets.py."""

from __future__ import annotations

from pathlib import Path

from scripts.check_spec_tickets import (
    STATUS_VALUES,
    check_indexes,
    check_tickets,
    main,
    stale_warnings,
)

VALID_V2 = """# 05: demo

**Blocked by:** None

**Gate:** `v2-portfolio`
**Signal set:** `unchanged`

**Status:** ready-for-agent
"""

VALID_NONE_COMPLETE = """# 07: demo

**Blocked by:** None

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-human

- [x] first acceptance item
"""


def _write(root: Path, name: str, body: str) -> Path:
    path = root / "docs" / "specs" / "demo" / "issues" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def _write_index(root: Path, rows: str) -> Path:
    path = root / "docs" / "specs" / "demo" / "issues" / "README.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "# 票索引\n\n| 票 | 主题 | 状态 | 依赖 |\n| --- | --- | --- | --- |\n" + rows,
        encoding="utf-8",
    )
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


def test_gate_none_complete_ticket_is_a_stale_warning(tmp_path: Path) -> None:
    path = _write(tmp_path, "07-complete.md", VALID_NONE_COMPLETE)
    warnings = stale_warnings(path)
    assert len(warnings) == 1
    assert "ready-for-human" in warnings[0]


def test_stale_warning_skips_gated_ticket(tmp_path: Path) -> None:
    body = VALID_V2.replace(
        "**Status:** ready-for-agent",
        "- [x] done\n\n**Status:** ready-for-agent",
    )
    path = _write(tmp_path, "05-gated.md", body)
    assert stale_warnings(path) == []


def test_stale_warning_skips_open_acceptance_box(tmp_path: Path) -> None:
    body = VALID_NONE_COMPLETE + "- [ ] remaining item\n"
    path = _write(tmp_path, "07-open.md", body)
    assert stale_warnings(path) == []


def test_stale_warning_skips_done_ticket(tmp_path: Path) -> None:
    body = VALID_NONE_COMPLETE.replace("ready-for-human", "done")
    path = _write(tmp_path, "07-done.md", body)
    assert stale_warnings(path) == []


def test_main_reports_stale_warning_without_failing(tmp_path: Path, capsys) -> None:
    _write(tmp_path, "07-complete.md", VALID_NONE_COMPLETE)
    assert main([str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "WARN" in out
    assert "stale warning" in out


def test_status_vocabulary_matches_triage_labels() -> None:
    doc = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "agents"
        / "triage-labels.md"
    ).read_text(encoding="utf-8")
    for status in STATUS_VALUES:
        assert f"`{status}`" in doc


def test_main_exit_codes(tmp_path: Path, capsys) -> None:
    _write(tmp_path, "05-valid.md", VALID_V2)
    assert main([str(tmp_path)]) == 0
    assert "OK: 1 ticket" in capsys.readouterr().out

    _write(tmp_path, "06-bad.md", "# demo\n")
    assert main([str(tmp_path)]) == 1
    assert "FAIL" in capsys.readouterr().out


def test_index_status_matches_ticket(tmp_path: Path) -> None:
    _write(tmp_path, "05-valid.md", VALID_V2)
    _write_index(tmp_path, "| [05](05-valid.md) | demo | ready-for-agent | — |\n")
    assert check_indexes(tmp_path) == {}


def test_index_status_mismatch_is_reported(tmp_path: Path) -> None:
    _write(tmp_path, "05-valid.md", VALID_V2)
    _write_index(tmp_path, "| [05](05-valid.md) | demo | done | — |\n")
    failures = check_indexes(tmp_path)
    assert "README says 'done', file says 'ready-for-agent'" in " ".join(
        next(iter(failures.values()))
    )


def test_index_missing_ticket_is_reported(tmp_path: Path) -> None:
    _write_index(tmp_path, "| [05](05-missing.md) | demo | done | — |\n")
    failures = check_indexes(tmp_path)
    assert "does not exist" in " ".join(next(iter(failures.values())))


def test_index_missing_status_is_reported(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "05-valid.md",
        VALID_V2.replace("**Status:** ready-for-agent\n", ""),
    )
    _write_index(tmp_path, "| [05](05-valid.md) | demo | ready-for-agent | — |\n")
    failures = check_indexes(tmp_path)
    assert "missing **Status:**" in " ".join(next(iter(failures.values())))


def test_invalid_status_is_reported(tmp_path: Path) -> None:
    _write(tmp_path, "05-valid.md", VALID_V2.replace("ready-for-agent", "in-progress"))
    failures = check_tickets(tmp_path)
    assert "invalid Status" in " ".join(next(iter(failures.values())))
